# Spark-Practice
This is my practice of Spark to manipulate data files with differnt spark APIs

## Practice List
* K-Means algorithm (with reservoir sampling) -- org.richardqiao.spark.practice.kmean
* Dataframe & dataset -- org.richardqiao.spark.practice.TimeUsage (dataset: http://alaska.epfl.ch/~dockermoocs/bigdata/atussum.csv)
* WordCount Top 20 -- org.richardqiao.spark.practice.WordCount
* Secondary Sort -- org.richardqiao.spark.practice.SecondarySort (dataset: datasets/9112770_T_ONTIME.zip)
* StatCounter Analysis -- org.richardqiao.spark.practice.StatCounterAnalysis (dataset: refered in the class file)

## Configurations
set HADOOP_HOME=D:\develop\packages\hadoop-common-2.2.0-bin-32bit-master\hadoop-common-2.2.0-bin-32bit
set _JAVA_OPTIONS=-Xmx4g

## Datasets
http://alaska.epfl.ch/~dockermoocs/bigdata/atussum.csv
datasets/9112770_T_ONTIME.zip

## Spark Issues:
* Encoders.bean can't consume Boolean type. Solution: Change all Boolean to boolean in your bean class.
* Encoders.bean can't consume @Transient fields. Solution: Write code to remove Transient fields from java bean code file.

