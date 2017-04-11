# Spark-Practice
This is my practice of Spark to manipulate data files with differnt spark APIs

set HADOOP_HOME=D:\develop\packages\hadoop-common-2.2.0-bin-32bit-master\hadoop-common-2.2.0-bin-32bit

set _JAVA_OPTIONS=-Xmx4g

http://alaska.epfl.ch/~dockermoocs/bigdata/atussum.csv


# Spark Issues:
* Encoders.bean can't consume Boolean type. Solution: Change all Boolean to boolean in your bean class.
* Encoders.bean can't consume @Transient fields. Solution: Write code to remove Transient fields from java bean code file.
