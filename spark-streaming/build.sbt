name := "SparkStreamingJob"

version := "1.0"

scalaVersion := "2.13.16"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.4" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.4.4" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.4.4" % "provided" ,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.4",
  "org.apache.spark" %% "spark-token-provider-kafka-0-10" % "3.4.4",
  "com.lihaoyi" %% "upickle" % "1.4.0",
  "org.xerial" % "sqlite-jdbc" % "3.36.0.3"
)

// 若在本地IDE中直接运行，可能需要将部分依赖从 provided 改为 compile
// 视实际部署和提交模式而定