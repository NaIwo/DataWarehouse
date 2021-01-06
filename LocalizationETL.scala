package com.example.bigdata

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, row_number}


object LocalizationETL {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("LocalizationETL")
      //.master("local")
      .enableHiveSupport()
      .getOrCreate()

    val username = System.getProperty("user.name");
    import spark.implicits._
    val  localizationMountain = spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(s"/user/$username/labs/spark/externaldata/geoDataMountain.csv");

    val  localizationPacific = spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(s"/user/$username/labs/spark/externaldata/geoDataPacific.csv");

    val  localizationCentral = spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(s"/user/$username/labs/spark/externaldata/geoDataCentral.csv");

    val  localizationEastern = spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(s"/user/$username/labs/spark/externaldata/geoDataEastern.csv");


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


    val concatMountain = mainMountain.select(col("Zipcode") as "Zip", col("Street") as "Street").join(localizationMountain, col("Zipcode") === col("Zip"), "leftouter");
    val concatPacific = mainPacific.select(col("Zipcode") as "Zip", col("Street") as "Street").join(localizationPacific, col("Zipcode") === col("Zip"), "leftouter");
    val concatCentral = mainCentral.select(col("Zipcode") as "Zip", col("Street") as "Street").join(localizationCentral, col("Zipcode") === col("Zip"), "leftouter");
    val concatEastern = mainEastern.select(col("Zipcode") as "Zip", col("Street") as "Street").join(localizationEastern, col("Zipcode") === col("Zip"), "leftouter");

    val localizationDF = concatMountain
      .union(concatPacific)
      .union(concatCentral)
      .union(concatEastern)
      .distinct();

    val allDataDF = localizationDF.
      withColumnRenamed("Zipcode", "Zip_code").
      withColumnRenamed("Street", "Street").
      withColumnRenamed("City", "City").
      withColumnRenamed("County", "County").
      withColumnRenamed("State", "State").
      withColumnRenamed("Country", "Country").
      withColumnRenamed("Timezone", "Timezone").
      withColumn("Localization_id", monotonically_increasing_id).
      select("Localization_id", "Zip_code", "Street", "City", "County", "State", "Country", "Timezone")

    val window = Window.orderBy($"Localization_id")

    val finalDataDF = allDataDF.withColumn("Localization_id", row_number.over(window))

    finalDataDF.write.insertInto("Localization")

    println("Załadowano dane do tabeli wymiaru 'Localization'")
  }
}