package org.richardqiao.spark.practice.kmean

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.annotation.tailrec
import scala.reflect.ClassTag

object Test {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local[*]").setAppName("hoho")
    val sc = new SparkContext(conf)
    val file = "e:/datasets/coursera-spark-epfl/stackoverflow.csv"
    val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    val langSpread: Int = 50000
    val kmeansKernels = langs.length * 3
    val kmeansETA = 20.0D
    val kmeansMaxIterations = 120
    val kmeansConvergence = 20.0D
//    val lines = sc.textFile(file)
//    val posts = lines.map(line =>{
//      val arr = line.split(',')
//      Posting(arr(0).toInt, arr(1).toInt,
//          if(arr(2).length() == 0) None else Some(arr(2).toInt),
//          if(arr(3).length() == 0) None else Some(arr(3).toInt),
//          arr(4).toInt,
//          if (arr.length > 5) Some(arr(5).intern()) else None)
//    }).filter(p => p.PostingType == 1 && p.tags != None)
//      .map(p => (p.tags.get, p))
//      .groupBy(_._1)
//      
//    val map = posts.mapValues(p => p.size)
//
//    map.collect.foreach(println)
    var count = 0
    
    val str = langs.maxBy { x =>
      {
        count += 1
        x.length()
      }
    }
    println(str)

  }
}