package Datapipeline

import java.util.UUID.randomUUID

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._


object StockAnalysisDataPipeline {
  /**
   * The function will provide the schema of the records that are present in
   * the stream.
   *
   * @return the structure of the record in a stream
   */
  def getStocksSchema(): StructType = {

    // Defining the structure of the record in stream
    val stock_data_schema = StructType(
      Array(
        StructField("symbols_requested", IntegerType),
        StructField("symbols_returned", IntegerType),
        StructField("data", ArrayType(
          StructType(
            Array(
              StructField("symbol", StringType),
              StructField("name", StringType),
              StructField("currency", StringType),
              StructField("price", StringType),
              StructField("price_open", StringType),
              StructField("day_high", StringType),
              StructField("day_low", StringType),
              StructField("col_52_week_high", StringType),
              StructField("col_52_week_low", StringType),
              StructField("day_change", StringType),
              StructField("change_pct", StringType),
              StructField("close_yesterday", StringType),
              StructField("market_cap", StringType),
              StructField("volume", StringType),
              StructField("volume_avg", StringType),
              StructField("shares", StringType),
              StructField("stock_exchange_long", StringType),
              StructField("stock_exchange_short", StringType),
              StructField("timezone", StringType),
              StructField("timezone_name", StringType),
              StructField("gmt_offset", StringType),
              StructField("last_trade_time", StringType),
              StructField("pe", StringType),
              StructField("eps", StringType)
            )
          )
        )
        )

      )
    )

    // Returning the structure.
    stock_data_schema

  }

  /**
   * The entry point of the program. It is responsible for performing the transformations
   * and storing the data into the Cassandra and MongoDB
   *
   * @param args : The command line arguments that will be passed to the program
   */
  def main(args: Array[String]): Unit = {

    println("Real-Time Data Pipeline Started ...")

    // DataPipelineConfiguration contains the configurations required to run the spark
    // application
    val conf = new DataPipelineConfiguration()
    // Getting the spark session
    // To run locally
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Stocks Data Analysis")
      .getOrCreate()

//    // To run on Cluster
//    val spark = SparkSession.builder
//      .appName("Stocks Data Analysis")
//      .getOrCreate()

    // Setting the logging level to ERROR
    spark.sparkContext.setLogLevel("ERROR")

    // Getting the streaming data from spark
    val stocks_data_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", conf.KAFKA_TOPIC_NAME)
      .option("startingOffsets", "latest")
      .load()

    // Getting the schema of the incoming data
    val stock_data_schema = getStocksSchema()

    // Since the data coming from the kafka will be in binary format, so converting it into
    // string
    val unstructured_stock_data = stocks_data_df.selectExpr("CAST(value AS STRING)")

    // A user define function that will give the UUID. UUID will be used as Primary Key in
    // Cassandra
    val uuid = udf(() => java.util.UUID.randomUUID().toString)

    // To convert the data into desired format. It will add a column that shows the date and
    // time of last trade of each stocks in UTC. This time will be calculate using
    // the "last_trade_time" and "timezone_name" columns from the incoming data. Adding a
    // column, named as 'pk', that will provide the unique id to each record
    val structured_stock_data = unstructured_stock_data
      .select(from_json(col("value"), stock_data_schema).as("all_stocks_detail"))
      .select(col("all_stocks_detail.*"))
      .select(col("data"), col("symbols_requested"), col("symbols_returned"))
      .withColumn("data", explode(col("data")))
      .select(col("data.*"), col("symbols_requested"), col("symbols_returned"))
      .withColumn("last_trade_time_utc",
        when( col("last_trade_time")
          .rlike("[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]) (2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9]"),
          to_utc_timestamp(col("last_trade_time"), col("timezone_name"))
        )
      ).withColumn("pk", uuid()).filter(col("stock_exchange_long").isNotNull)

    // Writing the ingested data into cassandra without any modification
    structured_stock_data.writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.write
          .format("org.apache.spark.sql.cassandra")
          .mode("append")
          .option("spark.cassandra.connection.host", conf.CASSANDRA_CONNECTION_HOST)
          .option("spark.cassandra.connection.port", conf.CASSANDRA_CONNECTION_PORT)
          .option("keyspace", conf.CASSANDRA_KEYSPACE_NAME)
          .option("table", conf.CASSANDRA_TABLE_NAME)
          .save()
      }.start()

    // Converting the incoming data into desired format
    val refined_stock_data = structured_stock_data.selectExpr(
      "symbol", "name", "currency",
      "cast(price as double) price",
      "cast(price_open as double) price_open",
      "cast(day_high as double) day_high",
      "cast(day_low as double) day_low",
      "cast(col_52_week_high as double) col_52_week_high",
      "cast(col_52_week_low as double) col_52_week_low",
      "cast(day_change as double) day_change",
      "cast(change_pct as double) change_pct",
      "cast(close_yesterday as double) close_yesterday",
      "cast(market_cap as double) market_cap",
      "cast(volume as double) volume",
      "cast(volume_avg as double) volume_avg",
      "cast(shares as double) shares",
      "stock_exchange_long",
      "stock_exchange_short",
      "timezone",
      "timezone_name",
      "gmt_offset",
      "last_trade_time",
      "pe",
      "eps",
      "last_trade_time_utc",
      "symbols_requested",
      "symbols_returned",
      "last_trade_time_utc",
      "pk"
    )

    // Creating the MongoDB URI. This URI will be used in storing the transformed data into MongoDB
    val spark_mongodb_output_uri = "mongodb://" + conf.MONGODB_USER + ":" + conf.MONGODB_PASSWORD + "@" +
                                    conf.MONGODB_HOST_NAME + ":" + conf.MONGODB_PORT + "/" + conf.MONGODB_DATABASE +
                                    ".stocks"
    println("Printing spark_mongodb_output_uri: " + spark_mongodb_output_uri)

    // Writing the transformed data into mongoDB
    refined_stock_data.writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.write
          .format("mongo")
          .mode("append")
          .option("uri", spark_mongodb_output_uri)
          .option("database", conf.MONGODB_DATABASE)
          .option("collection", "stocks")
          .save()
      }.start()

    // To display the transformed data on the console
    val stocks_data_write_stream = refined_stock_data
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("update")
      .option("truncate", "false")
      .format("console")
      .start()

    // waiting for the termination signal from user
    stocks_data_write_stream.awaitTermination()

    println("Real-Time Data Pipeline Completed.")

  }
}
