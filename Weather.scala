package com.example.bigdata

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, row_number}

import org.apache.spark.sql.functions.{date_format, split, to_date, unix_timestamp}


object Weather {

  def getAllInformation(line: String): String = {
    val pattern = "^On (.+) (.+) at the weather station at the airport (.+) the following weather conditions were noted: Temperature \\(F\\): (.+), Wind Chill \\(F\\): (.+), Humidity \\(%\\): (.+), Pressure \\(in\\): (.+), Visibility \\(miles\\): (.+), Wind Direction: (.+), Wind Speed \\(mph\\): (.+), Precipitation \\(in\\): (.+), Weather Condition: (.+)$".r

    line match {
      case pattern(date, time, airport, temperature, windChill, humidity, pressure, visibility, windDirection, windSpeed, precipitation, weatherCondition) =>  airport + ";" + temperature + ";" + humidity + ";" + visibility + ";" + weatherCondition
      case _ => "None"
    }
  }

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("WatherETL")
      //.master("local")
      .enableHiveSupport()
      .getOrCreate()

    val username = System.getProperty("user.name");
    import spark.implicits._


    val  weather = spark.read.textFile(s"/user/$username/labs/spark/externaldata/weather.txt").
      map(line => getAllInformation(line)).
      filter(!_.equals("None")).
      withColumn("splited", split($"value", ";")).
      select(
        //$"splited".getItem(0).as("Airport_Code"),
        $"splited".getItem(1).as("Temperature").cast("float"),
        $"splited".getItem(2).as("Humidity").cast("float"),
        $"splited".getItem(3).as("Visibility").cast("float"),
        $"splited".getItem(4).as("Weather_condition"))
      .distinct();

    val allDataDF = weather.
      withColumn("Weather_id", monotonically_increasing_id).
      select("Weather_id", "Temperature", "Humidity", "Visibility", "Weather_condition")

    val window = Window.orderBy($"Weather_id")

    val finalDataDF = allDataDF.withColumn("Weather_id", row_number.over(window))

    //finalDataDF.show()
    finalDataDF.write.insertInto("Weather")


    println("Załadowano dane do tabeli wymiaru 'Weather'")
  }
}