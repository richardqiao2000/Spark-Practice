package org.richardqiao.spark.practice

import java.nio.file.Paths

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object TimeUsage {
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.expressions.scalalang.typed

  val ss = SparkSession.builder.appName("Time Usage").master("local[*]").getOrCreate
  import ss.implicits._

  def main(args: Array[String]): Unit = {
    val file = ss.sparkContext.textFile("E:/Datasets/coursera-spark-epfl/atussum.csv")
    val headers = file.first.split(",").toList
    val firstField = StructField(headers.head, StringType, nullable = false)
    val fields = headers.tail.map(StructField(_, DoubleType, nullable = false))
    val schema = StructType(firstField :: fields)
    val data = file.mapPartitionsWithIndex((i, d) => if(i == 0) d.drop(1) else d)
                   .map(_.split(",").toList)
                   .map(list => Row.fromSeq(list.head :: list.tail.map(_.toDouble)))
    val df = ss.createDataFrame(data, schema)
    val primaryPre: List[String] = List("t01", "t03", "t11", "t1801", "t1803")
    val workingPre = List("t05", "t1805")
    val otherPre = List("t02", "t04", "t06", "t07", "t08", "t09", "t10", "t12", "t13", "t14", "t15", "t16", "t18")
    
    val (pCols, wCols, oCols) = headers.foldRight[(List[Column], List[Column], List[Column])](Nil, Nil, Nil){
      (col, list) =>
        if(primaryPre.exists(col.startsWith)) list.copy(_1 = new Column(col) :: list._1)
        else if(workingPre.exists(col.startsWith)) list.copy(_2 = new Column(col) :: list._2)
        else if(otherPre.exists(col.startsWith)) list.copy(_3 = new Column(col) :: list._3)
        else list
    }

    val colStatus: Column = when($"telfs" >= 1 && $"telfs" < 3, "working").otherwise("not working").as("status")
    val colAge: Column = when($"teage" <= 22, "young").when($"teage" >= 23 && $"teage" <= 55, "active").otherwise("elder").as("age")
    val colSex: Column = when($"tesex" === 1, "male").otherwise("female").as("sex")
    val colPrimary = pCols.reduce(_+_).divide(60).as("primary")
    val colWorking = wCols.reduce(_+_).divide(60).as("working")
    val colOther = oCols.reduce(_+_).divide(60).as("other")
    
    val df2 = df.select(colStatus, colAge, colSex, colPrimary, colWorking, colOther).where($"telfs" <= 4).cache
    val df3 = df2.groupBy("status", "age", "sex").agg(round(avg("primary"), 1), round(avg("working"), 1), round(avg("other"), 1)).orderBy("status", "age", "sex").cache
    //df3.show
    
    df2.createOrReplaceTempView("summed")
    val df4 = ss.sql(s"""
      select status, age, sex,
        round(avg(primary), 1),
        round(avg(working), 1),
        round(avg(other), 1)
      from summed
      group by status, age, sex
      order by status, age, sex
	  """).cache
	  //df4.show
	  
	  val ds = timeUsageSummaryTyped(df2)
	  val ds2 = timeUsageGroupedTyped(ds)
	  ds2.show
  }

  def timeUsageSummaryTyped(timeUsageSummaryDf: DataFrame): Dataset[TimeUsageRow] ={
    timeUsageSummaryDf.map(row => TimeUsageRow(row.getAs("status"),
                                               row.getAs("sex"),
                                               row.getAs("age"),
                                               row.getAs("primary"),
                                               row.getAs("working"),
                                               row.getAs("other"))) 
  }

  /**
    * @return Same as `timeUsageGrouped`, but using the typed API when possible
    * @param summed Dataset returned by the `timeUsageSummaryTyped` method
    *
    * Note that, though they have the same type (`Dataset[TimeUsageRow]`), the input
    * dataset contains one element per respondent, whereas the resulting dataset
    * contains one element per group (whose time spent on each activity kind has
    * been aggregated).
    *
    * Hint: you should use the `groupByKey` and `typed.avg` methods.
    */
  def timeUsageGroupedTyped(summed: Dataset[TimeUsageRow]): Dataset[TimeUsageRow] = {
    import org.apache.spark.sql.expressions.scalalang.typed
    summed.groupByKey(tur => tur.working + "," + tur.sex + "," + tur.age)
          .agg(typed.avg[TimeUsageRow](_.primaryNeeds), typed.avg[TimeUsageRow](_.work), typed.avg[TimeUsageRow](_.other))
          .map(d => (d._1.split(",").toList, d._2, d._3, d._4))
          .map(d => TimeUsageRow(d._1(0), d._1(1), d._1(2), Math.round(d._2 * 10) / 10d, Math.round(d._3 * 10) / 10d, Math.round(d._4 * 10) / 10d))
          .orderBy("working", "age", "sex")
  }

}



/**
  * Models a row of the summarized data set
  * @param working Working status (either "working" or "not working")
  * @param sex Sex (either "male" or "female")
  * @param age Age (either "young", "active" or "elder")
  * @param primaryNeeds Number of daily hours spent on primary needs
  * @param work Number of daily hours spent on work
  * @param other Number of daily hours spent on other activities
  */
case class TimeUsageRow(
  working: String,
  sex: String,
  age: String,
  primaryNeeds: Double,
  work: Double,
  other: Double
)