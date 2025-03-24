package myreddit.sparkstreaming

import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

import java.sql.{Connection, DriverManager, ResultSet}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import upickle.default._
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

    // 2) 从Kafka读取reddit_comments话题的数据
    //    注意：需根据生产环境的bootstrap servers来配置
    val kafkaBootstrapServers = "localhost:9092"
    val kafkaTopic = "reddit_comments"

    val rawDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "latest") 
      .load()

    // Kafka读取到的数据会有key、value、topic、partition等列；我们只关心value
    // value 是字节数组，需要转成String再进行JSON解析
    val dfWithJsonStr = rawDF.selectExpr("CAST(value AS STRING) as jsonStr")

    // 3) 解析JSON，提取字段；这里使用foreachBatch实现微批处理
    //    方便在微批完成后，将结果写回SQLite
    val query = dfWithJsonStr.writeStream
      .trigger(Trigger.ProcessingTime("30 seconds")) // 每5分钟触发一次微批
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        // 在这里对batchDF进行处理
        // a) 将JSON字符串反序列化为 RedditComment
        val redditComments = batchDF.collect().flatMap { row =>
          println("Debug JSON string:")
          println(row.getAs[String]("jsonStr"))
          parseJsonToRedditComment(row.getAs[String]("jsonStr"))
        }

        // b) 对评论做分词统计
        //    我们将结果先聚合到: Map(post_id -> Map(word -> freq))
        val aggregatedWordFreq: Map[String, Map[String, Long]] =
          redditComments.groupBy(_.post_id).map { case (pid, comments) =>
            // 将所有body的单词拆分，统计词频
            val allWords = comments.flatMap { cm =>
              tokenizeWords(cm.body)
            }
            val freqMap = allWords.groupBy(identity).mapValues(_.size.toLong).toMap
            (pid, freqMap)
          }

        // c) 连接SQLite，将新产生的词频与analysis_results中的旧数据合并
        updateAnalysisResults(aggregatedWordFreq)
      }
      .start()

    query.awaitTermination()
    spark.stop()
  }

  /**
    * 使用 uPickle 解析单条JSON字符串，转换成 RedditComment。
    * 若解析失败或字段缺失，则返回None。
    */
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

  /**
    * 简单分词示例：以非字母数字字符进行split。
    * 生产环境可用更完善的分词库，或过滤停用词等。
    */
  def tokenizeWords(text: String): Seq[String] = {
    if (text == null || text.isEmpty) return Seq.empty
    text
      .toLowerCase
      .split("[^a-z0-9']+")
      .filter(_.nonEmpty)
      .toSeq
  }

  /**
    * 情感分析的占位函数 (stub)。
    * 此处仅定义函数签名，未实现具体逻辑。
    * 后续可在此做真实的情感评分计算，并返回结果。
    */
  def analyzeSentiment(body: String): Double = {
    // TODO: sentiment logic
    0.0
  }

  /**
    * 将本批次的词频与analysis_results表里已有的数据进行合并后写回数据库。
    * aggregatedWordFreq: Map[post_id, Map[word, freq]]
    */
  def updateAnalysisResults(aggregatedWordFreq: Map[String, Map[String, Long]]): Unit = {
    // SQLite 数据库路径
    val dbPath = "jdbc:sqlite:/Users/kaiyuyang/Desktop/redditData.db"
    var conn: Connection = null

    try {
      Class.forName("org.sqlite.JDBC")
      conn = DriverManager.getConnection(dbPath)
      conn.setAutoCommit(false)

      // 准备查询现有词频的语句
      val selectStmt = conn.prepareStatement(
        "SELECT word_freq_json FROM analysis_results WHERE post_id = ?"
      )

      // 准备插入或更新的语句 (INSERT OR REPLACE)
      val upsertStmt = conn.prepareStatement(
        """INSERT OR REPLACE INTO analysis_results
          |(post_id, word_freq_json, sentiment_result, updated_time)
          |VALUES (?, ?, ?, ?)
          |""".stripMargin
      )

      val now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

      // 遍历每个post_id的词频Map
      for ((postId, newFreqMap) <- aggregatedWordFreq) {
        // 1) 先查询旧值
        selectStmt.setString(1, postId)
        val rs = selectStmt.executeQuery()

        val mergedFreqMap: Map[String, Long] =
          if (rs.next()) {
            val oldJson = rs.getString("word_freq_json")
            // 解析旧的JSON为Map[String, Long]
            val oldMap = parseWordFreqJson(oldJson)
            mergeFreqMaps(oldMap, newFreqMap)
          } else {
            // 说明analysis_results表还没有这个post_id的记录
            newFreqMap
          }
        rs.close()

        // 2) 将合并后的词频转换回JSON
        val freqJson = write(mergedFreqMap)

        // 3) sentiment_result 暂时置空或默认值
        //    后续可在这里合并 sentiment 分析结果
        val sentimentResult = "" // 或者 "{}" / "N/A"

        // 4) 准备 upsert
        upsertStmt.setString(1, postId)
        upsertStmt.setString(2, freqJson)
        upsertStmt.setString(3, sentimentResult)
        upsertStmt.setString(4, now)
        upsertStmt.executeUpdate()
      }

      // 提交事务
      conn.commit()

      selectStmt.close()
      upsertStmt.close()

    } catch {
      case e: Exception =>
        if (conn != null) {
          try {
            conn.rollback()
          } catch {
            case _: Throwable => // ignore
          }
        }
        throw new RuntimeException("[SparkStreamingJob] Error updating analysis_results", e)

    } finally {
      if (conn != null) {
        try {
          conn.close()
        } catch {
          case _: Throwable => // ignore
        }
      }
    }
  }

  /**
    * 将旧词频和新词频合并，累加相同word的计数
    */
  def mergeFreqMaps(oldMap: Map[String, Long], newMap: Map[String, Long]): Map[String, Long] = {
    // 所有key合并，若key重复则求和
    (oldMap.keySet ++ newMap.keySet).map { key =>
      val oldVal = oldMap.getOrElse(key, 0L)
      val newVal = newMap.getOrElse(key, 0L)
      key -> (oldVal + newVal)
    }.toMap
  }

  /**
    * 从JSON字符串解析出 Map[String, Long]。
    * 若解析异常则返回空Map。
    */
  def parseWordFreqJson(jsonStr: String): Map[String, Long] = {
    if (jsonStr == null || jsonStr.trim.isEmpty) return Map.empty
    try {
      read[Map[String, Long]](jsonStr)
    } catch {
      case _: Throwable =>
        Map.empty
    }
  }
}