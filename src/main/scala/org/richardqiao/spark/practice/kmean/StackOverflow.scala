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
  val langSpread: Int = 50000
  val kmeansKernels = langs.length * 3
  val kmeansETA = 20.0D
  val kmeansMaxIterations = 120
  
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
    
    //5. Build initial sample cluster points by using reservoir sampling
    //val samples = sampleVectors(vectors)
    val samples = vectors.groupByKey.flatMap({case (lang, list) => 
                                           sampling(list.toIterator, 3).map((lang, _))
                                         }).collect
    
                                         
    //6. Iterate do KMeans algorithm
    val means = kmeans(samples, vectors, debug = true)
    
    //7. Collect Cluster results
    val results = clusterResults(means, vectors)
    printResults(results)
    
    //samples.foreach(println)
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
  

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
    val newMeans = means.clone() // you need to compute newMeans

    // TODO: Fill in the newMeans array
    val tmpMeans = vectors
      .map { p =>
        (findClosest(p, means), p)
      }
      .groupByKey()
      .mapValues(averageVectors)
      .collect()

    tmpMeans foreach (kv => newMeans update(kv._1, kv._2))

    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansETA
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      println("Reached max iterations!")
      newMeans
    }
  }


  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansETA


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }




  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(Int, Int)]): Array[(String, Double, Int, Int)] = {
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.groupByKey

    val median = closestGrouped.mapValues { vs =>
      //val langMap = vs.groupBy(_._1)
      /*
      val mostCommon = vs.groupBy(_._1).maxBy(qa => qa._2.size)
      val langLabel: String = langs(mostCommon._1 / langSpread) // most common language in the cluster
      val clusterSize: Int = vs.size
      val langPercent: Double = mostCommon._2.size.toDouble * 100 / clusterSize // percent of the questions in the most common language
      val sortedScores = vs.map(_._2).toVector.sorted
      val medianScore: Int =
        if (clusterSize % 2 == 1) sortedScores(clusterSize / 2)
        else (sortedScores(clusterSize / 2) + sortedScores(clusterSize / 2 - 1)) / 2
        * 
        */

      
      val m = vs.groupBy(_._1).maxBy(qa => qa._2.size)
      val langLabel: String   = langs(m._1 / langSpread) // most common language in the cluster
      val langPercent: Double =  m._2.size.toDouble * 100 / vs.size // percent of the questions in the most common language
      val clusterSize: Int    = vs.size
      val sorted = vs.map(_._2).toVector.sorted
      val medianScore: Int    = {
        if(sorted.size % 2 == 1){
          sorted(sorted.size / 2)
        }else{
          (sorted(sorted.size / 2) + sorted(sorted.size / 2 - 1)) / 2
        }
      }

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }

}
