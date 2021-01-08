package com.example.bigdata

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, hour, monotonically_increasing_id, row_number}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SparkSession, functions}

object Time {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("TimeETL")
      //.master("local")
      .enableHiveSupport()
      .getOrCreate()

    val username = System.getProperty("user.name");
    import spark.implicits._

    val mainMountain = spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(s"/user/$username/labs/spark/externaldata/mainDataMountain.csv");

    val mainPacific = spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(s"/user/$username/labs/spark/externaldata/mainDataPacific.csv");

    val mainCentral = spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(s"/user/$username/labs/spark/externaldata/mainDataCentral.csv");

    val mainEastern = spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(s"/user/$username/labs/spark/externaldata/mainDataEastern.csv");


    val startTimeDF = mainMountain
      .union(mainPacific)
      .union(mainCentral)
      .union(mainEastern)
      .select("Start_Time")

    val timeDF = startTimeDF.withColumn("Interval", (hour(col("Start_Time")) / 6).cast(IntegerType)).withColumn("Year", functions.year(col("Start_Time"))).withColumn("Month", functions.month(col("Start_Time"))).withColumn("Day", functions.dayofmonth(col("Start_Time"))).drop("Start_Time").distinct()

    val allTimeDF = timeDF.withColumn("Time_id", monotonically_increasing_id).select("Time_id", "Interval", "Day", "Month", "Year")

    val window = Window.orderBy($"Time_id")

    val finalDataDF = allTimeDF.withColumn("Time_id", row_number.over(window))

    finalDataDF.write.insertInto("Time")
    //finalDataDF.show()
    println("Załadowano dane do tabeli wymiaru 'Time'")

  }
}
