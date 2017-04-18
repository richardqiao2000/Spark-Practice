package org.richardqiao.spark.practice

import scala.util._
import org.apache.spark.util._

object StatCounterX extends Serializable{
  def apply(x: Double): StatCounterX ={
    val scx = new StatCounterX()
    if(x.equals(Double.NaN)) scx.missing += 1
    else scx.stats.merge(x)
    scx
  }
}

class StatCounterX extends Serializable{
  val stats: StatCounter = new StatCounter
  var missing: Long = 0
  def mergeX(os: StatCounterX): StatCounterX = {
    this.stats.merge(os.stats)
    this.missing += os.missing
    this
  }
  
  override def toString: String = {
    stats.toString + " Missing: " + missing
  }
}