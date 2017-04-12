package org.richardqiao.spark.practice.kmean

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
//import annotation.tailrec
import scala.reflect.ClassTag

case class Posting(
  PostingType: Int,
  id: Int,
  acceptedAnswer: Option[Int],
  parentId: Option[Int],
  score: Int,
  tags: Option[String]
) extends Serializable


object StackOverflow extends StackOverflow{
  @transient lazy val conf = new SparkConf().setMaster("local[*]").setAppName("KMean")
  @transient lazy val sc = new SparkContext(conf)
  
  //Download from http://alaska.epfl.ch/~dockermoocs/bigdata/stackoverflow.csv
  val file = "E:/Datasets/coursera-spark-epfl/stackoverflow.csv"
  
  def main(args: Array[String]): Unit = {
    val lines = sc.textFile(file)
    val posts = lines.map(line =>{
      val arr = line.split(',')
      Posting(arr(0).toInt, arr(1).toInt,
          if(arr(2).length() == 0) None else Some(arr(2).toInt),
          if(arr(3).length() == 0) None else Some(arr(3).toInt),
          arr(4).toInt,
          Some(arr(5)))
    })
    posts.take(10).foreach(println)
  }
}

class StackOverflow extends Serializable{
  
  
}