package org.richardqiao.spark.practice

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import scala.util._

object StatCounterAnalysis {
  case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)
  case class Scored(md: MatchData, score: Double)
  val spark = SparkSession.builder.appName("hi").master("local[*]").getOrCreate
  val sc = spark.sparkContext
  
  def main(args: Array[String]): Unit = {
    val lines = sc.textFile("D:/datasets/advanced spark/donation/block_*.csv")
    val rdd = lines.filter(!_.startsWith("\"id_1\"")).map(line =>{
        val arr = line.split(',')
        val scores = arr.slice(2, 11).map(num =>{
          if(num.equals("?")){
            Double.NaN
          }else{
            num.toDouble
          }
        })
        MatchData(arr(0).toInt, arr(1).toInt, scores, arr(11).toBoolean)
    }).cache
    //val rddM = rdd.filter(_.matched).map(_.scores)
    //val rddU = rdd.filter(!_.matched).map(_.scores)
    
    //val statsM = getStatCounterX(rddM)
    //val statsU = getStatCounterX(rddU)
    
    //val diff = statsM.zip(statsU).map{case(a,b) =>
    //    (a.missing + b.missing, math.abs(a.stats.mean - b.stats.mean), a.stats.stdev + b.stats.stdev)
    //  }
    
    val scoredRDD = rdd.map(md => {
      val score = Array(2,5,6,7,8).map(i => md.scores(i))
                  .map(d => if(Double.NaN.equals(d)) 0.0 else d).sum
      Scored(md, score)
    }).cache
    
    val scoredM = scoredRDD.filter(_.score >= 4.0).map(_.md.matched)
                  .countByValue
    scoredM.foreach(println)
  }
  
  
  
  def getStatCounterX(rdd: RDD[Array[Double]]): Array[StatCounterX] = {
    rdd.mapPartitions(iter => {
      val scx = iter.next.map(StatCounterX(_))
      while(iter.hasNext){
        scx.zip(iter.next).map{case(a, b) => a.mergeX(StatCounterX(b))}
      }
      Iterator(scx)
    }).reduce((a, b) => {
      a.zip(b).map{case(a, b) => a.mergeX(b)}
    })
  }
}