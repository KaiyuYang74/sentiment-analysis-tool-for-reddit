#!/usr/bin/env bash
#
# start_pipeline.sh
#
# Start Spark Streaming first, then start the Producer.
# Optionally clear the database and delete/recreate Kafka topic.

# --------------- Optional: Clear Database ---------------
# Do you want to start fresh every time? For development/testing only, uncomment these lines:
# echo "Clearing existing SQLite database..."
# rm -f /Users/kaiyuyang/Desktop/redditData.db

# --------------- Optional: Reset/Delete Kafka Topic ---------------
# To delete and recreate the Topic, ensure Kafka is running and know your bootstrap server.
# Example below, adjust according to your actual cluster command line tools:
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

# Wait a bit to let Spark fully start
sleep 5

echo "Starting Producer..."
export NODE_TLS_REJECT_UNAUTHORIZED=0
cd /Users/kaiyuyang/Desktop/sentiment-analysis-tool/producer || exit
node src/index.js