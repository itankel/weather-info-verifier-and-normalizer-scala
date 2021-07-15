package com.bdd.project.weather_verifier_normalizer.services

import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{ SparkSession}

object RowWeatherInfoToFile extends App {
  System.setProperty("HADOOP_USER_NAME", "hdfs");
  System.setProperty("hadoop.home.dir", "C:\\Hadoop\\winutils\\");

  private val WEATHER_INFO_ROW_TOPIC = "weather_info_latest_raw_data";
  private val spark = SparkSession.builder().appName("append application").master("local[*]").getOrCreate;
  private val bootstrapServer = "cnt7-naya-cdh63:9092";



  val inputDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServer)
    .option("subscribe", WEATHER_INFO_ROW_TOPIC)
    .load
    .select(col("value").cast(StringType).alias("value"))

  inputDF.printSchema();
  //root
  // |-- stationId: integer (nullable = true)
  // |-- data: array (nullable = true)
  // |    |-- element: struct (containsNull = true)
  // |    |    |-- datetime: string (nullable = true)
  // |    |    |-- channels: array (nullable = true)
  // |    |    |    |-- element: struct (containsNull = true)
  // |    |    |    |    |-- id: integer (nullable = true)
  // |    |    |    |    |-- name: string (nullable = true)
  // |    |    |    |    |-- alias: string (nullable = true)
  // |    |    |    |    |-- value: double (nullable = true)
  // |    |    |    |    |-- status: integer (nullable = true)
  // |    |    |    |    |-- valid: boolean (nullable = true)
  // |    |    |    |    |-- description: string (nullable = true)
  //


  val query = inputDF.writeStream.format("json").option("path", "C:\\tmp\\bdd\\json")
    .option("checkpointLocation", "C:\\tmp\\bdd\\checkpoints")
    .start()

  query.awaitTermination(60000)

  inputDF.writeStream
    .foreachBatch((df, batchId) => {
      //   df.show()
      df.cache
      // df.write.parquet("hdfs:....")
      df.write.format("kafka").save
      df.unpersist
    })
    .start()



  spark.close()

}

