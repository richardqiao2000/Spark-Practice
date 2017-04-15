package org.richardqiao.spark.practice.kmean

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.annotation.tailrec
import scala.reflect.ClassTag

case class Posting(
  PostingType: Int,
  id: Int,
  acceptedAnswer: Option[Int],
  parentId: Option[Int],
  score: Int,
  tags: Option[String]
) extends Serializable


object StackOverflow{
  @transient lazy val conf = new SparkConf().setMaster("local[*]").setAppName("KMean")
  @transient lazy val sc = new SparkContext(conf)
  
  //Download from http://alaska.epfl.ch/~dockermoocs/bigdata/stackoverflow.csv
  val file = "e:/datasets/coursera-spark-epfl/stackoverflow.csv"
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
  val langSpread: Int = 10000
  val kmeansKernels = langs.length * 3
  val kmeansETA = 20.0D
  val kmeansMaxIterations = 120
  val kmeansConvergence = 20.0D
  
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data")
  
  def main(args: Array[String]): Unit = {
    val lines = sc.textFile(file)
    
    //1. Convert to RDD[Posting]
    val posts = lines.map(line =>{
      val arr = line.split(',')
      Posting(arr(0).toInt, arr(1).toInt,
          if(arr(2).length() == 0) None else Some(arr(2).toInt),
          if(arr(3).length() == 0) None else Some(arr(3).toInt),
          arr(4).toInt,
          if (arr.length > 5) Some(arr(5).intern()) else None)
    }).cache
    
    //2. Join Questions and Anwers and make pairs
    //   Convert to [Int, [Question, Answer]]
    val questions = posts.filter(_.PostingType == 1).map(p => (p.id, p))
    val answers = posts.filter(p => p.PostingType == 2 && p.parentId != None)
                       .map(p => (p.parentId.get, p))
    val pairs = questions.join(answers).groupByKey.cache
    
    
    //3. Compute the maximum score for each posting
    //   [Posting, Int]
    val scores = pairs.map(p => (p._2.head._1, p._2.map(_._2.score)))
                      .map(p => (p._1, {
                        var max = Int.MinValue
                        for(x <- p._2){
                          if(max < x) max = x
                        }
                        max
                      })).filter(_._1.tags != None)
    
    //4. Build vector[Int, Int] to demonstrate language' score
    //   Each language is represented as an integer number
    val vectors = scores.map(s => ((if(s._1.tags == None) 0 else langs.indexOf(s._1.tags.get)) * langSpread, s._2))
    assert(vectors.count == 2121822, "Incorrect number of vector: " + vectors.count)
    
    //5. Build initial sample cluster kernels by using reservoir sampling
    //val samples = sampleVectors(vectors)
    val samples = vectors.groupByKey.flatMap({case (lang, list) => 
                                           sampling(list.toIterator, 3).map((lang, _))
                                         }).collect
    
                                         
    //6. Iterate do KMeans algorithm
    val means = kmeans(samples, vectors, 0)
    
    //7. Get Median Cluster results Array[(language, medianScore, language_percent, size)]
    val closest = vectors.map(v => (getClosestKernel(v, means), v)).groupByKey
    val medians = closest.map(_._2).map{list =>
        val lang = list.groupBy(_._1).maxBy(_._2.size)
        val language = langs(lang._1 / langSpread)
        val size = list.size
        val perc = lang._2.size.toDouble * 100 / size.toDouble
        val sorted = list.map(_._2).toVector.sorted
        val len = sorted.size
        val score = {
          if(len % 2 == 1) sorted(len / 2)
          else{
            val i = len / 2 - 1
            val j = len / 2
            (sorted(i) + sorted(j)) / 2
          }
        }
        (language, perc, size, score)
    }
    val results = medians.collect.sortBy(_._4)

    //8. Print results
    printResults(results)
    
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }

  def distance(p1: (Int, Int), p2: (Int, Int)): Long = {
    val xx = (p1._1 - p2._1).toLong * (p1._1 - p2._1).toLong
    val yy = (p1._2 - p2._2).toLong * (p1._2 - p2._2).toLong
    xx + yy
  }
  
  def getClosestKernel(p: (Int, Int), means: Array[(Int, Int)]): Int = {
    var index = 0
    var minDist = Long.MaxValue
    for(i <- 0 until means.length){
      val dist = distance(p, means(i))
      if(minDist > dist){
        minDist = dist
        index = i
      }
    }
    index
  }
  
  def getCenter(list: Iterable[(Int, Int)]): (Int, Int) = {
    var x: Long = 0
    var y: Long = 0
    val len = list.iterator.length
    for(p <- list){
      x += p._1
      y += p._2
    }
    ((x / len).toInt, (y / len).toInt)
  }
  
  def kmeans(lastMean: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1): Array[(Int, Int)] = {
    val next = lastMean.clone
    //1. Get next mean
    val newMeans = vectors.map(v => (getClosestKernel(v, next), v))
                      .groupByKey
                      .mapValues(getCenter)
                      .collect
    newMeans.foreach(kv => next.update(kv._1, kv._2))

    //2. getDistance away from last means
    var dist: Double = 0
    for(i <- 0 until next.length){
      val p1 = lastMean(i)
      val p2 = next(i)
      val xx: Double = (p1._1 - p2._1).toDouble
      val yy: Double = (p1._2 - p2._2).toDouble
      dist += xx * xx + yy * yy
    }
    
    //3. Call next cycle of kmeans or return
    if(dist < kmeansConvergence || iter > kmeansMaxIterations){
      next
    }else{
      kmeans(next, vectors, iter + 1)
    }
  }

  def sampling(list: Iterator[Int], size: Int): Array[Int] = {
    val res = new Array[Int](size)
    for(i <- 0 until size){
      assert(list.hasNext, "No enough data")
      res(i) = list.next
    }
    
    var sz = size
    val rnd = new scala.util.Random(langSpread)
    while(list.hasNext){
      val i = math.abs(rnd.nextInt) % sz
      val nxt = list.next
      if(i < size) res(i) = nxt
      sz += 1
    }
    res
  }
  
}
