package com.bdd.project.weather_verifier_normalizer.services

import com.bdd.project.weather_verifier_normalizer.model.{NormalizedStationCollectedData, StationCollectedData}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, element_at, explode, from_json}
import java.nio.file.FileSystems

import org.apache.log4j.{Level, Logger}


object RowWeatherInfoFromFileApp extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  println("RowWeatherInfoFromFileApp  -  start ")
  val curRunPath= FileSystems.getDefault.getPath("").toAbsolutePath
  println("RowWeatherInfoFromFileApp  -  curRunPath "+curRunPath)
  System.setProperty("HADOOP_USER_NAME", "hdfs");
  System.setProperty("hadoop.home.dir", "C:\\Hadoop\\winutils\\");

  val sparkSession: SparkSession = SparkSession.builder.master("local[*]").appName("example_app").getOrCreate


  val schema = Encoders.product[StationCollectedData].schema
  val fromFileDF = sparkSession.read
    .schema(schema)
    .json("C:\\Users\\User\\course\\BigDataDeveloper\\project\\final_project\\weather-info-verifier-and-normilzer-scala\\data\\jsontoread\\*")

  println("----------------------------------------------------------------------")
  println(fromFileDF.printSchema())



  println("----------------------------------------------------------------------")
  fromFileDF.show()

  var flattenedDF = fromFileDF
    .withColumn("station_data", element_at(col("data"), 1))
    .drop("data")

  flattenedDF = flattenedDF.select(col("stationId"), col("station_data")("datetime").as("datetime"),
    col("station_data")("channels").as("channels"))

  flattenedDF.printSchema()
  flattenedDF.show()

  flattenedDF=flattenedDF.withColumn("channeli", explode(flattenedDF.col("channels")))

  flattenedDF.printSchema()
  flattenedDF.show()

  flattenedDF=flattenedDF
    .withColumn("channel",flattenedDF.col("channeli.name"))
    .withColumn("value",flattenedDF.col("channeli.value"))
    .withColumn("status",flattenedDF.col("channeli.status"))
    .withColumn("valid",flattenedDF.col("channeli.valid"))
    .drop("channels")
    .drop("channeli")


  flattenedDF.printSchema()
  flattenedDF.show(false)


  import sparkSession.implicits._

  val validChannelsNames: Seq[String] = Seq("BP", "DiffR", "Grad", "NIP","Rain","RH","STDwd","TD","TDmax","TDmin","TG","Time","WD","WDmax","WS","Ws10mm","WS1mm","WSmax")
  val normalizedStationCollectedDS=flattenedDF.as[NormalizedStationCollectedData]
    .filter(_.isValid(validChannelsNames))
    .drop("valid")
    .drop("status")

  println("count the number of valid data" + normalizedStationCollectedDS.count())
  println("----------------------------------------------------------------------")
  normalizedStationCollectedDS.show(false)

  val query = normalizedStationCollectedDS
    .write.format("json")
    .option("path", "C:\\Users\\User\\course\\BigDataDeveloper\\project\\final_project\\weather-info-verifier-and-normilzer-scala\\data\\valid_data\\outputfile\\stations_data")
    .option("checkpointLocation", "C:\\Users\\User\\course\\BigDataDeveloper\\project\\final_project\\weather-info-verifier-and-normilzer-scala\\data\\valid_data\\checkpoints\\")
    .save


println(" after saved tofile ")

//
//
//  private val WEATHER_INFO_ROW_TOPIC = "weather_info_latest_raw_data";
//  private val spark = SparkSession.builder().appName("append application").master("local[*]").getOrCreate;
//
//  private val bootstrapServer = "cnt7-naya-cdh63:9092";
//
//
//
//  val inputDF = spark
//    .readStream
//    .format("kafka")
//    .option("kafka.bootstrap.servers", bootstrapServer)
//    .option("subscribe", WEATHER_INFO_ROW_TOPIC)
//    .load
//    .select(col("value").cast(StringType).alias("value"))
//
//  inputDF.printSchema();
//  //root
//  // |-- stationId: integer (nullable = true)
//  // |-- data: array (nullable = true)
//  // |    |-- element: struct (containsNull = true)
//  // |    |    |-- datetime: string (nullable = true)
//  // |    |    |-- channels: array (nullable = true)
//  // |    |    |    |-- element: struct (containsNull = true)
//  // |    |    |    |    |-- id: integer (nullable = true)
//  // |    |    |    |    |-- name: string (nullable = true)
//  // |    |    |    |    |-- alias: string (nullable = true)
//  // |    |    |    |    |-- value: double (nullable = true)
//  // |    |    |    |    |-- status: integer (nullable = true)
//  // |    |    |    |    |-- valid: boolean (nullable = true)
//  // |    |    |    |    |-- description: string (nullable = true)
//  //
//
//
//  val stationCollectedDataEncoder = Encoders.product[StationCollectedData]
//  val inSchema = stationCollectedDataEncoder.schema
//
//  val newDF = inputDF
//    .withColumn("parsed_json", from_json(col("value"), inSchema))
//    .select("parsed_json.*")
//
//  var flatDF = newDF.select(col("stationId"), col("data"))
//    .withColumn("station_data", element_at(col("data"), 1)).drop("data")
//  flatDF = flatDF.select(col("stationId"), col("station_data")("datetime").as("datetime"),
//    col("station_data")("channels").as("channels"))
//
//  flatDF.printSchema()
//    // this is what I have now in my hands
//    //root
//    // |-- stationId: integer (nullable = true)
//    // |-- datetime: string (nullable = true)
//    // |-- channels: array (nullable = true)
//    // |    |-- element: struct (containsNull = true)
//    // |    |    |-- id: integer (nullable = true)
//    // |    |    |-- name: string (nullable = true)
//    // |    |    |-- alias: string (nullable = true)
//    // |    |    |-- value: double (nullable = true)
//    // |    |    |-- status: integer (nullable = true)
//    // |    |    |-- valid: boolean (nullable = true)
//    // |    |    |-- description: string (nullable = true)
//
//
//  //TODO target based on  Yevgenie's
//  // each station as its  N   channels
//  // for example station 1 can have 4 channels  vs   station 2 can have 20 channels
//  //
//
//  //TODO I need to reach :
//  //TODO
//  // root
//  // |-- stationId: integer (nullable = true)
//  // |-- datetime: string (nullable = true)
//  // |-- name: string (nullable = true)
//  // |-- value: double (nullable = false)
//  // |-- status: integer (nullable = true)
//  // |-- valid: boolean (nullable = true)
//
//
//  //TODO then I will validate the data  and remove not valid data  ( or may be I need to do it in a different stage ?)
//  //  - valid
//  //  - status
//
//  //TODO after validation transform to :
//
//  //TODO
//  // root
//  // |-- stationId: integer (nullable = true)
//  // |-- datetime: string (nullable = true)
//  // |-- name: string (nullable = true)
//  // |-- value: double (nullable = false)
//
//
//  inputDF.writeStream
//    .foreachBatch((df, batchId) => {
//      //   df.show()
//      df.cache
//      // df.write.parquet("hdfs:....")
//      df.write.format("kafka").save
//      df.unpersist
//    })
//    .start()
//
//  spark.close()

}

