package com.bdd.project.weather_verifier_normalizer.services

import com.bdd.project.weather_verifier_normalizer.model.{NormalizedStationCollectedData, StationCollectedData}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, element_at, explode, from_json}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.streaming.StreamingQueryException
import org.slf4j.LoggerFactory

object RowWeatherInfoApp extends App {
  System.setProperty("HADOOP_USER_NAME", "hdfs");
  System.setProperty("hadoop.home.dir", "C:\\Hadoop\\winutils\\");

  private val log = LoggerFactory.getLogger(classOf[Any])
  private val WEATHER_INFO_ROW_TOPIC = "weather_info_latest_raw_data";
  private val sparkSession = SparkSession.builder().appName("append application").master("local[*]").getOrCreate;

  private val bootstrapServer = "cnt7-naya-cdh63:9092";

  val inputDF = sparkSession
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

  val stationCollectedDataEncoder = Encoders.product[StationCollectedData]
  val inSchema = stationCollectedDataEncoder.schema

  var newDF = inputDF
    .withColumn("parsed_json", from_json(col("value"), inSchema))
    .select("parsed_json.*")

  newDF = newDF.select(col("stationId"), col("data"))
    .withColumn("station_data", element_at(col("data"), 1)).drop("data")

  newDF = newDF.select(col("stationId"), col("station_data")("datetime").as("datetime"),
    col("station_data")("channels").as("channels"))

  newDF.printSchema()
  println("-----------------------------------0--------------------------------")

 // newDF.show()
  newDF=newDF.withColumn("channeli", explode(newDF.col("channels")))

  newDF.printSchema()
  println("-----------------------------------1--------------------------------")

 // newDF.show()

  newDF=newDF
    .withColumn("channel",newDF.col("channeli.name"))
    .withColumn("value",newDF.col("channeli.value"))
    .withColumn("status",newDF.col("channeli.status"))
    .withColumn("valid",newDF.col("channeli.valid"))
    .drop("channels")
    .drop("channeli")


  newDF.printSchema()
  println("-----------------------------------2--------------------------------")

 // newDF.show(false)


  import sparkSession.implicits._
  val validChannelsNames: Seq[String] = Seq("BP", "DiffR", "Grad", "NIP","Rain","RH","STDwd","TD","TDmax","TDmin","TG","Time","WD","WDmax","WS","Ws10mm","WS1mm","WSmax")
  val normalizedStationCollectedDS=newDF.as[NormalizedStationCollectedData]
    .filter(_.isValid(validChannelsNames))
    .drop("valid")
    .drop("status")

//  println("count the number of valid data" + normalizedStationCollectedDS.count())
  println("-------------------------------------3---------------------------------")

//  normalizedStationCollectedDS.show(false)
//
  val query = normalizedStationCollectedDS.toJSON
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServer)
    .option("checkpointLocation", "C:\\tmp\\bdd\\checkpoints")
    .option("topic", "weather_info_verified_data")
    .option("mode","update")
    .option("numRows", 3)
    .start()




  try query.awaitTermination(60000)
  catch {
    case e: StreamingQueryException =>
      log.error("Exception while waiting for query to end {}."+ e.getMessage+" - "+ e.toString())
  } finally {
    sparkSession.close
  }

  log.debug("  out of the start ---------------------------")
  //writing to console
  //  val query = normalizedStationCollectedDS
  //    .writeStream
  //    .outputMode(OutputMode.Append)
  //    .format("console")
  //    .option("truncate", false)
  //    .option("numRows", 3)
  //    .start();

//  inputDF.writeStream
//    .foreachBatch((df, batchId) => {
//      //   df.show()
//      df.cache
//      // df.write.parquet("hdfs:....")
//      df.write.format("kafka").save
//      df.unpersist
//    })
//    .start()


}

// this is what I have now in my hands
//root
// |-- stationId: integer (nullable = true)
// |-- datetime: string (nullable = true)
// |-- channels: array (nullable = true)
// |    |-- element: struct (containsNull = true)
// |    |    |-- id: integer (nullable = true)
// |    |    |-- name: string (nullable = true)
// |    |    |-- alias: string (nullable = true)
// |    |    |-- value: double (nullable = true)
// |    |    |-- status: integer (nullable = true)
// |    |    |-- valid: boolean (nullable = true)
// |    |    |-- description: string (nullable = true)


//TODO target based on  Yevgenie's
// each station as its  N   channels
// for example station 1 can have 4 channels  vs   station 2 can have 20 channels
//

//TODO I need to reach :
//TODO
// root
// |-- stationId: integer (nullable = true)
// |-- datetime: string (nullable = true)
// |-- name: string (nullable = true)
// |-- value: double (nullable = false)
// |-- status: integer (nullable = true)
// |-- valid: boolean (nullable = true)


//TODO then I will validate the data  and remove not valid data  ( or may be I need to do it in a different stage ?)
//  - valid
//  - status

//TODO after validation transform to :

//TODO
// root
// |-- stationId: integer (nullable = true)
// |-- datetime: string (nullable = true)
// |-- name: string (nullable = true)
// |-- value: double (nullable = false)
