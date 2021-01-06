package com.example.bigdata

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, row_number}


object Day {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("LocalizationETL")
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

    val dayDF = mainMountain
      .union(mainPacific)
      .union(mainCentral)
      .union(mainEastern)
      .select("Sunrise_Sunset", "Civil_Twilight", "Nautical_Twilight", "Astronomical_Twilight")
      .distinct().na.drop("all");


    val allDayDF = dayDF.
      withColumn("Day_id", monotonically_increasing_id).
      select("Day_id", "Sunrise_Sunset", "Civil_Twilight", "Nautical_Twilight", "Astronomical_Twilight")

    val window = Window.orderBy($"Day_id")

    val finalDataDF = allDayDF.withColumn("Day_id", row_number.over(window))

    finalDataDF.show()
    finalDataDF.write.insertInto("Day")

    println("Załadowano dane do tabeli wymiaru 'Day'")
  }
}