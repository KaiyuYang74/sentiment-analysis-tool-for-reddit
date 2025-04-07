package myreddit.sparkstreaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

import java.sql.{Connection, DriverManager}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import upickle.default._

// 引入 STTP
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

    // 1) 初始化Spark
    val spark = SparkSession.builder()
      .appName("SparkStreamingJob")
      // 如果你要在本地测试，可加 .master("local[*]")
      .getOrCreate()

    // 2) 从Kafka读取 reddit_comments 话题的数据
    val kafkaBootstrapServers = "localhost:9092"
    val kafkaTopic = "reddit_comments"

    val rawDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "latest")
      .load()

    // 仅保留value字段（字节数组 -> String）
    val dfWithJsonStr = rawDF.selectExpr("CAST(value AS STRING) as jsonStr")

    // 3) 在foreachBatch里处理
    val query = dfWithJsonStr.writeStream
      .trigger(Trigger.ProcessingTime("30 seconds")) // 每30秒触发一次微批
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val redditComments = batchDF.collect().flatMap { row =>
          val jsonStr = row.getAs[String]("jsonStr")
          parseJsonToRedditComment(jsonStr)
        }

        // 对评论做分词统计
        val aggregatedWordFreq: Map[String, Map[String, Long]] =
          redditComments.groupBy(_.post_id).map { case (pid, comments) =>
            val allWords = comments.flatMap { cm => tokenizeWords(cm.body) }
            val freqMap = allWords.groupBy(identity).mapValues(_.size.toLong).toMap
            (pid, freqMap)
          }

        // ============ 情感分析部分 ============
        // 逐条分析，然后取平均等逻辑
        val aggregatedSentiment: Map[String, String] =
        redditComments.groupBy(_.post_id).map { case (pid, comments) =>
            // 逐条分析：对每条评论调用 analyzeSentiment
            val numericScores = comments.map { cm =>
            val label = analyzeSentiment(cm.body)
            // 将情感标签映射为数值分数
            val score = label match {
                case "positive" => 1.0
                case "negative" => -1.0
                case _ => 0.0
            }
            score
            }

            // 取所有评论分数的平均值
            val avgScore = if (numericScores.nonEmpty) numericScores.sum / numericScores.size else 0.0

            // 根据平均分映射回标签，可根据实际需求调整阈值
            val sentimentLabel = if (avgScore > 0.1) {
            "positive"
            } else if (avgScore < -0.1) {
            "negative"
            } else {
            "neutral"
            }

            (pid, sentimentLabel)
        }

        // 写入数据库：包含词频和sentiment
        updateAnalysisResults(aggregatedWordFreq, aggregatedSentiment)
      }
      .start()

    query.awaitTermination()
    spark.stop()
  }

  // ========== JSON 解析(不变) ==========
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

  // ========== 简单分词 ==========
  def tokenizeWords(text: String): Seq[String] = {
    if (text == null || text.isEmpty) return Seq.empty
    text
      .toLowerCase
      .split("[^a-z0-9']+")
      .filter(_.nonEmpty)
      .toSeq
  }

  /**
    * 通过 HTTP 调用 Python REST 服务，获取情感结果。
    * 返回值： "positive" / "negative" / "neutral" / 其他自定义。
    * 可根据需要映射到数值等。
    */
  def analyzeSentiment(body: String): String = {
    // 若空文本，直接返回 "neutral" 或其他占位
    if(body == null || body.trim.isEmpty) return "neutral"

    // 构造请求
    val sentimentUrl = "http://localhost:5001/api/sentiment"
    val requestBody = ujson.Obj("text" -> body).toString()
    val backend = HttpURLConnectionBackend()

    val response = basicRequest
      .post(uri"$sentimentUrl")
      .body(requestBody)
      .contentType(MediaType.ApplicationJson)
      .send(backend)

    if (response.isSuccess) {
      // 解析返回的 JSON: {"sentiment":"positive"} / "negative"/"neutral" ...
      try {
// 从Either里取出成功或错误字符串
            val responseBody: String = response.body.fold(
            err => err,   // 如果是Left，返回错误字符串
            ok  => ok     // 如果是Right，返回成功字符串
            )
            // 然后解析
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
    * 将词频和情感结果写入analysis_results表。
    * @param aggregatedWordFreq   Map[post_id, Map[word, freq]]
    * @param aggregatedSentiment  Map[post_id, String]，存储情感标签
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

      // 准备查询现有词频
      val selectStmt = conn.prepareStatement(
        "SELECT word_freq_json FROM analysis_results WHERE post_id = ?"
      )

      // 准备插入或更新 (INSERT OR REPLACE)
      val upsertStmt = conn.prepareStatement(
        """INSERT OR REPLACE INTO analysis_results
          |(post_id, word_freq_json, sentiment_result, updated_time)
          |VALUES (?, ?, ?, ?)
          |""".stripMargin
      )

      val now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

      // 遍历每个 post_id
      for ((postId, newFreqMap) <- aggregatedWordFreq) {
        selectStmt.setString(1, postId)
        val rs = selectStmt.executeQuery()

        // merge 旧的词频
        val mergedFreqMap: Map[String, Long] =
          if (rs.next()) {
            val oldJson = rs.getString("word_freq_json")
            val oldMap = parseWordFreqJson(oldJson)
            mergeFreqMaps(oldMap, newFreqMap)
          } else {
            newFreqMap
          }
        rs.close()

        // 转为 JSON
        val freqJson = write(mergedFreqMap)
        // sentiment 结果
        val sentimentLabel = aggregatedSentiment.getOrElse(postId, "unknown")

        upsertStmt.setString(1, postId)
        upsertStmt.setString(2, freqJson)
        upsertStmt.setString(3, sentimentLabel) // 这里直接存文本标签，也可存score
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

  /** 将旧词频和新词频合并，累加相同word的计数 */
  def mergeFreqMaps(oldMap: Map[String, Long], newMap: Map[String, Long]): Map[String, Long] = {
    (oldMap.keySet ++ newMap.keySet).map { key =>
      val oldVal = oldMap.getOrElse(key, 0L)
      val newVal = newMap.getOrElse(key, 0L)
      key -> (oldVal + newVal)
    }.toMap
  }

  /** 解析 word_freq_json => Map[String, Long] */
  def parseWordFreqJson(jsonStr: String): Map[String, Long] = {
    if (jsonStr == null || jsonStr.trim.isEmpty) return Map.empty
    try {
      read[Map[String, Long]](jsonStr)
    } catch {
      case _: Throwable => Map.empty
    }
  }
}