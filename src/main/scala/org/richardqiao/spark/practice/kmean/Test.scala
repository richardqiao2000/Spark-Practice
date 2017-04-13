package org.richardqiao.spark.practice.kmean

object Test {
  def main(args: Array[String]): Unit ={
    val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    val i = langs.indexOf("Scala")
    println(i)
  }
}