name := "RealTimeStockDataAnalysis"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2",
  "org.mongodb.spark" %% "mongo-spark-connector" % sparkVersion
)