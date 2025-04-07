#!/usr/bin/env bash
#
# start_pipeline.sh
#
# 先启动 Spark Streaming，再启动 Producer。
# 可选地清空数据库和删除重建 Kafka topic。

# --------------- 可选操作：清空数据库 ---------------
# 是否每次都要重新来过？若只在开发/测试时用，可取消注释以下两行：
# echo "Clearing existing SQLite database..."
# rm -f /Users/kaiyuyang/Desktop/redditData.db

# --------------- 可选操作：重置/删除 Kafka Topic ---------------
# 若要删除并重建 Topic，需要先确保 Kafka 已启动，并且知道bootstrap server。
# 下面操作仅供示例，注意和你的实际集群命令行工具对应：
# KAFKA_TOPIC="reddit_comments"
# BOOTSTRAP_SERVER="localhost:9092"
# echo "Deleting Kafka topic $KAFKA_TOPIC..."
# kafka-topics.sh --delete --topic "$KAFKA_TOPIC" --bootstrap-server "$BOOTSTRAP_SERVER"
# echo "Recreating Kafka topic $KAFKA_TOPIC..."
# kafka-topics.sh --create --topic "$KAFKA_TOPIC" --bootstrap-server "$BOOTSTRAP_SERVER" --partitions 1 --replication-factor 1

echo "Starting Spark Streaming job..."
spark-submit \
  --class myreddit.sparkstreaming.SparkStreamingJob \
  --master "local[*]" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.4,\
org.apache.spark:spark-token-provider-kafka-0-10_2.13:3.4.4,\
org.xerial:sqlite-jdbc:3.36.0.3,\
com.lihaoyi:upickle_2.13:1.4.0,\
com.softwaremill.sttp.client3:core_2.13:3.8.3 \
  /Users/kaiyuyang/Projects/sentiment-analysis-tool/spark-streaming/target/scala-2.13/sparkstreamingjob_2.13-1.0.jar &
# spark-submit \
#   --class myreddit.sparkstreaming.SparkStreamingJob \
#   --master "local[*]" \
#   --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/Users/kaiyuyang/Projects/sentiment-analysis-tool/my_spark_conf/log4j.properties" \
#   --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/Users/kaiyuyang/Projects/sentiment-analysis-tool/my_spark_conf/log4j.properties" \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.4,\
# org.apache.spark:spark-token-provider-kafka-0-10_2.13:3.4.4,\
# org.xerial:sqlite-jdbc:3.36.0.3,\
# com.lihaoyi:upickle_2.13:1.4.0,\
# com.softwaremill.sttp.client3:core_2.13:3.8.3 \
#   /Users/kaiyuyang/Projects/sentiment-analysis-tool/spark-streaming/target/scala-2.13/sparkstreamingjob_2.13-1.0.jar &

SPARK_STREAMING_PID=$!
echo "Spark Streaming job started with PID: $SPARK_STREAMING_PID"

# 可以先稍等一下，让Spark完全启动
sleep 5

echo "Starting Producer..."
export NODE_TLS_REJECT_UNAUTHORIZED=0
cd /Users/kaiyuyang/Projects/sentiment-analysis-tool/producer || exit
node src/index.js