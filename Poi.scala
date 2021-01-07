package com.example.bigdata

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, row_number}


object Poi {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("PoiETL")
      //.master("local")
      .enableHiveSupport()
      .getOrCreate()

    val username = System.getProperty("user.name");
    import spark.implicits._


    val  mainMountain = spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(s"/user/$username/labs/spark/externaldata/mainDataMountain.csv");

    val  mainPacific = spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(s"/user/$username/labs/spark/externaldata/mainDataPacific.csv");

    val  mainCentral = spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(s"/user/$username/labs/spark/externaldata/mainDataCentral.csv");

    val  mainEastern = spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(s"/user/$username/labs/spark/externaldata/mainDataEastern.csv");

    val PoiDF = mainMountain
      .union(mainPacific)
      .union(mainCentral)
      .union(mainEastern)
      .select("Amenity",
        "Bump",
        "Crossing",
        "Give_Way",
        "Junction",
        "No_Exit",
        "Railway",
        "Roundabout",
        "Station",
        "Stop",
        "Traffic_Calming",
        "Traffic_Signal",
        "Turning_Loop")
      .distinct().na.drop("all");


    val intPoiDF = PoiDF.select(
      (col("Amenity").cast("integer") + col("Railway").cast("integer") + col("Station").cast("integer")).as("Object_poi_count"),
      (col("Bump").cast("integer") + col("Traffic_Calming").cast("integer") + col("Traffic_Signal").cast("integer")).as("Calming_poi_count"),
      (col("Crossing").cast("integer") + col("Junction").cast("integer") + col("Roundabout").cast("integer") + col("Turning_Loop").cast("integer")).as("Road_poi_count"),
      (col("Give_Way").cast("integer") + col("No_Exit").cast("integer") + col("Stop").cast("integer")).as("Sign_poi_count")
    );
    val allPoiDF = intPoiDF.
      withColumn("Poi_id", monotonically_increasing_id).
      select("Poi_id", "Object_poi_count", "Calming_poi_count", "Road_poi_count", "Sign_poi_count");


    val window = Window.orderBy($"Poi_id")

    val finalDataDF = allPoiDF.withColumn("Poi_id", row_number.over(window))
    //finalDataDF.show()
    finalDataDF.write.insertInto("Poi")

    println("Załadowano dane do tabeli wymiaru 'Poi'")
  }
}