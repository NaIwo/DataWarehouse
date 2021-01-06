# DataWarehouse
PUT project: BigData

## uruchamianie skryptu hurtownia.scala

spark-shell -i hurtownia.scala

## Uruchamianie ETL'i

spark-submit --class com.example.bigdata.LocalizationETL \\
--master yarn --num-executors 5 --driver-memory 512m \\
--executor-memory 512m --executor-cores 1 LocalizationETL.jar
