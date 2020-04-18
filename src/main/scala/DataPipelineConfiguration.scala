package Datapipeline

class DataPipelineConfiguration {
  val KAFKA_TOPIC_NAME = "transmessage"
  val KAFKA_BOOTSTRAP_SERVERS = "34.71.248.10:9092"

  val MONGODB_HOST_NAME = "34.71.248.10"
  val MONGODB_PORT = "27017"
  val MONGODB_USER = "stocksuser"
  val MONGODB_PASSWORD = "stocksuser"
  val MONGODB_DATABASE = "stocks_db"

  val CASSANDRA_CONNECTION_HOST = "34.71.248.10"
  val CASSANDRA_CONNECTION_PORT = "9042"
  val CASSANDRA_KEYSPACE_NAME = "stocks_ks"
  val CASSANDRA_TABLE_NAME = "stocks"




}
