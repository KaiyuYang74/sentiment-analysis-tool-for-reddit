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
  "org.xerial" % "sqlite-jdbc" % "3.36.0.3",
  "com.softwaremill.sttp.client3" %% "core" % "3.8.3" // STTP client
)

// If running directly in local IDE, you might need to change some dependencies from provided to compile
// Depends on actual deployment and submission mode