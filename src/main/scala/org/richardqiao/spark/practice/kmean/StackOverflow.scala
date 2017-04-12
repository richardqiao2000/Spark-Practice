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
  //data format:
  //<postTypeId>,<id>,[<acceptedAnswer>],[<parentId>],<score>,[<tag>]
  //1,27233496,,,0,C#
  //1,23698767,,,9,C#
  //1,5484340,,,0,C#
  //2,5494879,,5484340,1,
  //1,9419744,,,2,Objective-C
  //1,26875732,,,1,C#
  //1,9002525,,,2,C++
  //2,9003401,,9002525,4,
  //2,9003942,,9002525,1,
  //2,9005311,,9002525,0,
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