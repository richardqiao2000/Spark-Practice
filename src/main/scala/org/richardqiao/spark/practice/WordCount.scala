package org.richardqiao.spark.practice

import org.apache.spark.rdd._
import org.apache.spark._
import scala.util._
import scala.io.Source
import org.jsoup._

object WordCount {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val url = "https://www.usatoday.com/story/news/2017/04/11/airlines-detail-flight-rules-contracts-of-carriage/100331176/"
    val web = Source.fromURL(url).mkString.toLowerCase
    val str = Jsoup.parse(web).text()
    val rdd = sc.parallelize(List(str)).flatMap(_.split(" "))
                .map((_, 1)).reduceByKey(_+_)
    val sort = rdd.takeOrdered(20)(Ordering[Int].reverse.on(_._2))
    sort.foreach(println)
    //sc.parallelize(List(str)).flatMap(_.split(" ")).collect().foreach(println)
  }
}