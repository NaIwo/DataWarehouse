package com.example.bigdata

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{abs, col, count, hour, min, monotonically_increasing_id, round, row_number, split, to_timestamp, unix_timestamp}
import org.apache.spark.sql.types.IntegerType

object FactsTable {

  def getAllInformation(line: String): String = {
    val pattern = "^On (.+) (.+) at the weather station at the airport (.+) the following weather conditions were noted: Temperature \\(F\\): (.+), Wind Chill \\(F\\): (.+), Humidity \\(%\\): (.+), Pressure \\(in\\): (.+), Visibility \\(miles\\): (.+), Wind Direction: (.+), Wind Speed \\(mph\\): (.+), Precipitation \\(in\\): (.+), Weather Condition: (.+)$".r

    line match {
      case pattern(date, time, airport, temperature, windChill, humidity, pressure, visibility, windDirection, windSpeed, precipitation, weatherCondition) =>  airport + ";" + temperature + ";" + humidity + ";" + visibility + ";" + weatherCondition+ ";" + date + " " + time
      case _ => "None"
    }
  }
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("FactsTable")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    val username = System.getProperty("user.name");

    import spark.implicits._
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    //////////////////////////

    ///////////////////////


    val weatherDF = spark.sql("SELECT * FROM Weather")

    val  weatherWithTimestamp = spark.read.textFile(s"/user/$username/labs/spark/externaldata/weather.txt").
      map(line => getAllInformation(line)).
      filter(!_.equals("None")).
      withColumn("splited", split($"value", ";")).
      select(
        $"splited".getItem(0).as("AC"),
        $"splited".getItem(1).as("Temperature").cast("float"),
        $"splited".getItem(2).as("Humidity").cast("float"),
        $"splited".getItem(3).as("Visibility").cast("float"),
        $"splited".getItem(4).as("Weather_condition"),
        $"splited".getItem(5).as("timestampString")
      )
      .withColumn("timestamp", to_timestamp($"timestampString", "yyyy-MM-dd HH:mm:ss.S"))
      .withColumn("Year", functions.year(col("timestampString")))
      .withColumn("Month", functions.month(col("timestampString")))
      .withColumn("Day", functions.dayofmonth(col("timestampString")))
      .withColumn("Hour", hour(col("timestampString")))

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

    val allAccidenst = mainMountain
      .union(mainPacific)
      .union(mainCentral)
      .union(mainEastern)
      .dropDuplicates(Array("ID"));

    val allAccidentsWithTime = allAccidenst
      .withColumn("accidentTimestmp", to_timestamp($"Start_Time"))
      .withColumn("Interval", (hour(col("Start_Time")) / 6).cast(IntegerType))
      .withColumn("Year", functions.year(col("Start_Time")))
      .withColumn("Month", functions.month(col("Start_Time")))
      .withColumn("Day", functions.dayofmonth(col("Start_Time")))
      .withColumn("Hour", hour(col("Start_Time")))
      .withColumn("Object_poi_count", col("Amenity").cast("integer") + col("Railway").cast("integer") + col("Station").cast("integer"))
      .withColumn("Calming_poi_count", col("Bump").cast("integer") + col("Traffic_Calming").cast("integer") + col("Traffic_Signal").cast("integer"))
      .withColumn("Road_poi_count", col("Crossing").cast("integer") + col("Junction").cast("integer") + col("Roundabout").cast("integer") + col("Turning_Loop").cast("integer"))
      .withColumn("Sign_poi_count", col("Give_Way").cast("integer") + col("No_Exit").cast("integer") + col("Stop").cast("integer"))


    ////tu jest git weatherWithTimestamp
    val weatherDFWithTimeDiff = allAccidentsWithTime.join(weatherWithTimestamp,
      weatherWithTimestamp("AC") === allAccidentsWithTime("Airport_Code") &&
        allAccidentsWithTime("Year") === weatherWithTimestamp("Year") &&
        allAccidentsWithTime("Month") === weatherWithTimestamp("Month") &&
        allAccidentsWithTime("Day") === weatherWithTimestamp("Day") &&
        abs(allAccidentsWithTime("Hour") - weatherWithTimestamp("Hour")) <= 5)
      .select($"ID", $"Airport_Code",  allAccidentsWithTime("Start_Time"), (unix_timestamp($"Start_Time") - unix_timestamp($"timestampString")).as("timeDifferent"))
      .groupBy($"ID")
      .agg(min("timeDifferent").as("minTimeDifferent"))

    val accWeather =  allAccidentsWithTime.
      join(weatherDFWithTimeDiff, allAccidentsWithTime("ID") === weatherDFWithTimeDiff("ID"), "left")
      .join(weatherWithTimestamp, weatherWithTimestamp("AC") === allAccidentsWithTime("Airport_Code")
        && allAccidentsWithTime("Year") === weatherWithTimestamp("Year")
        && allAccidentsWithTime("Month") === weatherWithTimestamp("Month")
        && allAccidentsWithTime("Day") === weatherWithTimestamp("Day")
        && (unix_timestamp($"Start_Time") - unix_timestamp($"timestampString")) === weatherDFWithTimeDiff("minTimeDifferent"), "left")
      .select(allAccidentsWithTime("ID"), $"Temperature", $"Humidity", $"Visibility", $"Weather_condition")


    val weatherConnected = accWeather.join(weatherDF,
      weatherDF("Humidity") === accWeather("Humidity") &&
      weatherDF("Temperature") === accWeather("Temperature") &&
      weatherDF("Visibility") === accWeather("Visibility") &&
      weatherDF("Weather_condition") === accWeather("Weather_condition"), "left")
      .select(accWeather("ID").as("id"), weatherDF("Weather_id"))



    val dayDF = spark.sql("SELECT * FROM Day")
    val poiDF = spark.sql("SELECT * FROM Poi")
    val timeDF = spark.sql("SELECT * FROM Time")
    val localizationDF = spark.sql("SELECT Localization_id, Zip_code, Street FROM Localization")

    val dayConnected = allAccidentsWithTime.join(dayDF,
      dayDF("Sunrise_Sunset")===allAccidentsWithTime("Sunrise_Sunset") &&
        dayDF("Civil_Twilight")===allAccidentsWithTime("Civil_Twilight") &&
        dayDF("Nautical_Twilight")===allAccidentsWithTime("Nautical_Twilight") &&
        dayDF("Astronomical_Twilight")===allAccidentsWithTime("Astronomical_Twilight")
    ).select(allAccidentsWithTime("ID").as("id"), $"Day_id")

    val poiConnected = allAccidentsWithTime.join(poiDF,
      poiDF("Object_poi_count")===allAccidentsWithTime("Object_poi_count")&&
        poiDF("Calming_poi_count")===allAccidentsWithTime("Calming_poi_count")&&
        poiDF("Road_poi_count")===allAccidentsWithTime("Road_poi_count")&&
        poiDF("Sign_poi_count")===allAccidentsWithTime("Sign_poi_count")
    ).select(allAccidentsWithTime("ID").as("id"), $"Poi_id")

    val timeConnected = allAccidentsWithTime.join(timeDF,
      timeDF("Interval")===allAccidentsWithTime("Interval") &&
        timeDF("Year")===allAccidentsWithTime("Year") &&
        timeDF("Month")===allAccidentsWithTime("Month") &&
        timeDF("Day")===allAccidentsWithTime("Day")
    ).select(allAccidenst("ID").as("id"), $"Time_id")

    val localizationConnected = allAccidentsWithTime.join(localizationDF,
      localizationDF("Zip_code")===allAccidentsWithTime("Zipcode")&&
        localizationDF("Street")===allAccidentsWithTime("Street")
    ).select(allAccidentsWithTime("ID").as("id"), $"Localization_id")

    val finalTable = timeConnected
      .join(weatherConnected, weatherConnected("id") === timeConnected("id"))
      .join(dayConnected, dayConnected("id") === timeConnected("id"))
      .join(poiConnected, poiConnected("id") === timeConnected("id"))
      .join(localizationConnected, localizationConnected("id") === timeConnected("id"))
      .join(allAccidentsWithTime, allAccidentsWithTime("ID") === timeConnected("id"))
      .select(allAccidentsWithTime("ID").as("Accidents_id"), $"Time_id", $"Poi_id", $"Weather_id", $"Localization_id", $"Day_id", round((unix_timestamp($"End_Time") - unix_timestamp($"Start_Time"))/60).as("Accident_duration"))
    //finalDataDF.printSchema()
    //finalDataDF.show()
    finalTable.write.insertInto("Accidents")
    //finalTable.printSchema()
    println("Załadowano tabele faktow")



  }
}
