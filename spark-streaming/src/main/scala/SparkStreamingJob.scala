package myreddit.sparkstreaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

import java.sql.{Connection, DriverManager}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import upickle.default._

// Import STTP
import sttp.client3._
import sttp.model.MediaType

case class RedditComment(
  post_id: String,
  comment_id: String,
  author: String,
  body: String,
  created_utc: Long
)
object RedditComment {
  implicit val rw: ReadWriter[RedditComment] = macroRW
}

object SparkStreamingJob {

  def main(args: Array[String]): Unit = {

    // 1) Initialize Spark
    val spark = SparkSession.builder()
      .appName("SparkStreamingJob")
      // If testing locally, add .master("local[*]")
      .getOrCreate()

    // 2) Read from Kafka topic 'reddit_comments'
    val kafkaBootstrapServers = "localhost:9092"
    val kafkaTopic = "reddit_comments"

    val rawDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "latest")
      .load()

    // Keep only value field (byte array -> String)
    val dfWithJsonStr = rawDF.selectExpr("CAST(value AS STRING) as jsonStr")

    // 3) Process in foreachBatch
    val query = dfWithJsonStr.writeStream
      .trigger(Trigger.ProcessingTime("30 seconds")) // Trigger micro-batch every 30 seconds
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val redditComments = batchDF.collect().flatMap { row =>
          val jsonStr = row.getAs[String]("jsonStr")
          parseJsonToRedditComment(jsonStr)
        }

        // Word frequency analysis on comments
        val aggregatedWordFreq: Map[String, Map[String, Long]] =
          redditComments.groupBy(_.post_id).map { case (pid, comments) =>
            val allWords = comments.flatMap { cm => tokenizeWords(cm.body) }
            val freqMap = allWords.groupBy(identity).mapValues(_.size.toLong).toMap
            (pid, freqMap)
          }

        // ============ Sentiment Analysis ============
        // Analyze each comment, then calculate average
        val aggregatedSentiment: Map[String, String] =
        redditComments.groupBy(_.post_id).map { case (pid, comments) =>
            // Process each comment: call analyzeSentiment
            val numericScores = comments.map { cm =>
            val label = analyzeSentiment(cm.body)
            // Map sentiment label to numeric score
            val score = label match {
                case "positive" => 1.0
                case "negative" => -1.0
                case _ => 0.0
            }
            score
            }

            // Calculate average of all comment scores
            val avgScore = if (numericScores.nonEmpty) numericScores.sum / numericScores.size else 0.0

            // Map average score back to label, adjust thresholds as needed
            val sentimentLabel = if (avgScore > 0.1) {
            "positive"
            } else if (avgScore < -0.1) {
            "negative"
            } else {
            "neutral"
            }

            (pid, sentimentLabel)
        }

        // Write to database: word frequency and sentiment
        updateAnalysisResults(aggregatedWordFreq, aggregatedSentiment)
      }
      .start()

    query.awaitTermination()
    spark.stop()
  }

  // ========== JSON Parsing (unchanged) ==========
  def parseJsonToRedditComment(jsonStr: String): Option[RedditComment] = {
    try {
      val rc = read[RedditComment](jsonStr)
      Some(rc)
    } catch {
      case e: Throwable =>
        println(s"Parse error: ${e.getMessage} | json=$jsonStr")
        None
    }
  }

  // ========== Simple Tokenization ==========
  def tokenizeWords(text: String): Seq[String] = {
    if (text == null || text.isEmpty) return Seq.empty
    text
      .toLowerCase
      .split("[^a-z0-9']+")
      .filter(_.nonEmpty)
      .toSeq
  }

  /**
    * Call Python REST service via HTTP to get sentiment results.
    * Return values: "positive" / "negative" / "neutral" / others.
    * Can be mapped to numeric values if needed.
    */
  def analyzeSentiment(body: String): String = {
    // For empty text, return "neutral" or other placeholder
    if(body == null || body.trim.isEmpty) return "neutral"

    // Build request
    val sentimentUrl = "http://localhost:5001/api/sentiment"
    val requestBody = ujson.Obj("text" -> body).toString()
    val backend = HttpURLConnectionBackend()

    val response = basicRequest
      .post(uri"$sentimentUrl")
      .body(requestBody)
      .contentType(MediaType.ApplicationJson)
      .send(backend)

    if (response.isSuccess) {
      // Parse returned JSON: {"sentiment":"positive"} / "negative"/"neutral" ...
      try {
// Extract success or error string from Either
            val responseBody: String = response.body.fold(
            err => err,   // If Left, return error string
            ok  => ok     // If Right, return success string
            )
            // Then parse
            val json = ujson.read(responseBody)
        json("sentiment").str
      } catch {
        case _: Throwable => "unknown"
      }
    } else {
      println(s"[analyzeSentiment] HTTP Error: ${response.code} ${response.statusText}")
      "unknown"
    }
  }

  /**
    * Write word frequency and sentiment results to analysis_results table.
    * @param aggregatedWordFreq   Map[post_id, Map[word, freq]]
    * @param aggregatedSentiment  Map[post_id, String], storing sentiment labels
    */
  def updateAnalysisResults(
      aggregatedWordFreq: Map[String, Map[String, Long]],
      aggregatedSentiment: Map[String, String]
    ): Unit = {

    val dbPath = "jdbc:sqlite:/Users/kaiyuyang/Desktop/redditData.db?journal_mode=WAL&busy_timeout=60000"
    var conn: Connection = DriverManager.getConnection(dbPath)

    try {
      val statement = conn.createStatement()
      statement.execute("PRAGMA journal_mode = WAL")
      statement.execute("PRAGMA busy_timeout = 60000")
      statement.close()
      
      conn.setAutoCommit(false)

      // Prepare query for existing word frequencies
      val selectStmt = conn.prepareStatement(
        "SELECT word_freq_json FROM analysis_results WHERE post_id = ?"
      )

      // Prepare insert or update (INSERT OR REPLACE)
      val upsertStmt = conn.prepareStatement(
        """INSERT OR REPLACE INTO analysis_results
          |(post_id, word_freq_json, sentiment_result, updated_time)
          |VALUES (?, ?, ?, ?)
          |""".stripMargin
      )

      val now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

      // Process each post_id
      for ((postId, newFreqMap) <- aggregatedWordFreq) {
        selectStmt.setString(1, postId)
        val rs = selectStmt.executeQuery()

        // Merge old word frequencies
        val mergedFreqMap: Map[String, Long] =
          if (rs.next()) {
            val oldJson = rs.getString("word_freq_json")
            val oldMap = parseWordFreqJson(oldJson)
            mergeFreqMaps(oldMap, newFreqMap)
          } else {
            newFreqMap
          }
        rs.close()

        // Convert to JSON
        val freqJson = write(mergedFreqMap)
        // Sentiment result
        val sentimentLabel = aggregatedSentiment.getOrElse(postId, "unknown")

        upsertStmt.setString(1, postId)
        upsertStmt.setString(2, freqJson)
        upsertStmt.setString(3, sentimentLabel) // Store text label directly, could also store score
        upsertStmt.setString(4, now)
        upsertStmt.executeUpdate()
      }

      conn.commit()

      selectStmt.close()
      upsertStmt.close()

    } catch {
      case e: Exception =>
        if (conn != null) {
          try { conn.rollback() } catch { case _: Throwable => }
        }
        throw new RuntimeException("[SparkStreamingJob] Error updating analysis_results", e)
    } finally {
      if (conn != null) {
        try { conn.close() } catch { case _: Throwable => }
      }
    }
  }

  /** Merge old and new word frequencies, summing counts for same words */
  def mergeFreqMaps(oldMap: Map[String, Long], newMap: Map[String, Long]): Map[String, Long] = {
    (oldMap.keySet ++ newMap.keySet).map { key =>
      val oldVal = oldMap.getOrElse(key, 0L)
      val newVal = newMap.getOrElse(key, 0L)
      key -> (oldVal + newVal)
    }.toMap
  }

  /** Parse word_freq_json => Map[String, Long] */
  def parseWordFreqJson(jsonStr: String): Map[String, Long] = {
    if (jsonStr == null || jsonStr.trim.isEmpty) return Map.empty
    try {
      read[Map[String, Long]](jsonStr)
    } catch {
      case _: Throwable => Map.empty
    }
  }
}