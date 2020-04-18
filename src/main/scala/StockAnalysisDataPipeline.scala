package Datapipeline

import java.util.UUID.randomUUID

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._


object StockAnalysisDataPipeline {

  def getStocksSchema(): StructType = {

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

    stock_data_schema

  }

  def main(args: Array[String]): Unit = {
    println("Real-Time Data Pipeline Started ...")

    val conf = new DataPipelineConfiguration()

    // To run locally
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Stocks Data Analysis")
      .getOrCreate()

//    // To run on Cluster
//    val spark = SparkSession.builder
//      .appName("Stocks Data Analysis")
//      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val stocks_data_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", conf.KAFKA_TOPIC_NAME)
      .option("startingOffsets", "latest")
      .load()

    val stock_data_schema = getStocksSchema()

    val unstructured_stock_data = stocks_data_df.selectExpr("CAST(value AS STRING)")

    val uuid = udf(() => java.util.UUID.randomUUID().toString)

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

    val spark_mongodb_output_uri = "mongodb://" + conf.MONGODB_USER + ":" + conf.MONGODB_PASSWORD + "@" +
                                    conf.MONGODB_HOST_NAME + ":" + conf.MONGODB_PORT + "/" + conf.MONGODB_DATABASE +
                                    ".stocks"
    println("Printing spark_mongodb_output_uri: " + spark_mongodb_output_uri)

    refined_stock_data.writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        //val batchDF_1 = batchDF.withColumn("batch_id", lit(batchId))
        // Transform batchDF and write it to sink/target/persistent storage
        // Write data from spark dataframe to database

        batchDF.write
          .format("mongo")
          .mode("append")
          .option("uri", spark_mongodb_output_uri)
          .option("database", conf.MONGODB_DATABASE)
          .option("collection", "stocks")
          .save()
      }.start()

    val stocks_data_write_stream = refined_stock_data
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("update")
      .option("truncate", "false")
      .format("console")
      .start()
    // Code Block 4 Ends Here

    stocks_data_write_stream.awaitTermination()

    println("Real-Time Data Pipeline Completed.")

  }
}
