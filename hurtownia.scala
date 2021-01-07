spark.sql("""DROP TABLE IF EXISTS `Accidents`""")
spark.sql("""DROP TABLE IF EXISTS `Poi`""")
spark.sql("""DROP TABLE IF EXISTS `Weather`""")
spark.sql("""DROP TABLE IF EXISTS `Time`""")
spark.sql("""DROP TABLE IF EXISTS `Localization`""")
spark.sql("""DROP TABLE IF EXISTS `Day`""")

spark.sql("""CREATE TABLE `Accidents` (
 `Time_id` int,
 `Poi_id` int,
 `Airport_Code` int,
 `Localization_id` int,
 `Day_id` int,
 `Accidents_perH` double,
 `HyperLogLog` double
 )
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")


spark.sql("""CREATE TABLE `Poi` (
 `Poi_id` int,
 `Object_poi_count` int,
 `Calming_poi_count` int,
 `Road_poi_count` int,
`Sign_poi_count` int
 )
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")


spark.sql("""CREATE TABLE `Weather` (
 `Airport_Code` int,
 `Weather_description` string,
 `Bad_weather` boolean
 )
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE `Time` (
 `Time_id` int,
 `Interval_6H` double,
 `Day` int,
 `Month` int,
 `Year` int
 )
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE `Localization` (
 `Localization_id` int,
 `Zip_code` string,
 `Street` string,
 `City` string,
 `County` string,
 `State` string,
 `Country` string,
 `Timezone` string
 )
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")


spark.sql("""CREATE TABLE `Day` (
 `Day_id` int,
 `Sunrise_Sunset` string,
 `Civil_Twilight` string,
 `Nautical_Twilight` string,
 `Astronomical_Twilight` string
 )
 ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")