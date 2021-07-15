package com.bdd.project.weather_verifier_normalizer.test

import com.bdd.project.weather_verifier_normalizer.model.StationCollectedData
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

// just to see that it runs
object SparkScalaExample extends App {
  val sparkSession: SparkSession = SparkSession.builder.master("local[*]").appName("example_app").getOrCreate


  val schema = spark.sql.Encoders.product[StationCollectedData].schema
  val inputDF = sparkSession.read.option("multiline", "true").schema(schema).json("data/collected_ims_data.json")


  inputDF.printSchema()

  inputDF.show
  //  val spark: SparkSession = SparkSession.builder.master("local[*]").appName("example_app").getOrCreate
  //
  //  import spark.implicits._
  //
  //  val sampleData: Seq[String] = Seq("one", "two", "three", "four")
  //
  //  val dataDF: DataFrame = spark.createDataset(sampleData).toDF.withColumnRenamed("value", "my_col")
  //
  //  dataDF.show
  //
  //  spark.close
}
