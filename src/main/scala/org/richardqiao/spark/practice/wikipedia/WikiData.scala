package org.richardqiao.spark.practice.wikipedia

import java.io._

case class WikiArticle(title: String, text: String)

object WikiData {
  private[wikipedia] def filePath = {
    //File download from: http://alaska.epfl.ch/~dockermoocs/bigdata/wikipedia.dat
    //Data format:
    //"<page><title>Otto Piper (castle researcher)</title><text>#REDIRECT [[Otto Piper]]</text></page>",
    //"<page><title>Klessheim Palace</title><text>#REDIRECT [[Schloss Klessheim]]</text></page>",
    new File("E:/Datasets/coursera-spark-epfl/wikipedia.dat").getPath
  }
  
  private[wikipedia] def parse(line: String): WikiArticle = {
    val subs = "</title><text>"
    val i = line.indexOf(subs)
    val title = line.substring(14, i)
    val text = line.substring(i + subs.length, line.length - 16)
    WikiArticle(title, text)
  }
}