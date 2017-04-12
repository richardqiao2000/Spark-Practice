package org.richardqiao.spark.practice.wikipedia

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import java.io._

object WikiRanking {
  val langs = List("JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
  
  val conf = new SparkConf().setMaster("local[*]").setAppName("wiki ranking")
  val sc = new SparkContext(conf)
  
  def main(args: Array[String]): Unit = {
    val wikiRdd = sc.textFile(WikiData.filePath).map(WikiData.parse)
                    .map(_.text.split(' '))
                    .flatMap(list => langs.filter(list.contains(_)).map((_, 1)))
                    .reduceByKey(_+_)
                    .sortBy(-_._2)
    wikiRdd.collect().foreach(println)
  }
}