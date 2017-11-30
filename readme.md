# Smartmeter Analytics - Batch Layer
## Overview   
  
A Java library that calculates 5 minute, hourly & daily power consumption for each city & 5 min forecasts using Spark & time-series librray for Spark.

## Requirements
See [POM file](./pom.xml)


## Usage Example
```java
$SPARK_HOME/bin/spark-submit --master local --deploy-mode client --class com.khattak.bigdata.batch.sensordataanalytics.smartmeter.CalculateSmartmeterStatistics smartmeter-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar hdfs://localhost:8020/hadoop/data/smartmeter/raw_readings/2016/{month}/{day} hdfs://localhost:8020/hadoop/data/smartmeter/statistics/city_usage hdfs://localhost:8020/hadoop/data/smartmeter/forecasts/city_usage/input/smartmeter_readings_5_minute.csv
```

## License
The content of this project is licensed under the [Creative Commons Attribution 3.0 license](https://creativecommons.org/licenses/by/3.0/us/deed.en_US).