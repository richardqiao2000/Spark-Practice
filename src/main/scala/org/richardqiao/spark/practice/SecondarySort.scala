package org.richardqiao.spark.practice

import org.apache.spark._
import org.apache.spark.broadcast._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import com.google.common.collect.{HashBasedTable, Table}

object SecondarySort {
  val DAY_OF_MONTH = 0
  val DAY_OF_WEEK = 1
  val FL_DATE = 2
  val UNIQUE_CARRIER = 3
  val CARRIER = 4
  val ORIGIN_AIRPORT_ID = 5
  val ORIGIN_CITY_MARKET_ID = 6
  val ORIGIN_STATE_ABR = 7
  val DEST_AIRPORT_ID = 8
  val DEST_CITY_MARKET_ID = 9
  val DEST_STATE_ABR = 10
  val CRS_DEP_TIME = 11
  val DEP_TIME = 12
  val DEP_DELAY_NEW = 13
  val TAXI_OUT = 14
  val WHEELS_OFF = 15
  val WHEELS_ON = 16
  val TAXI_IN = 17
  val CRS_ARR_TIME = 18
  val ARR_TIME = 19
  val ARR_DELAY = 20
  val CANCELLED = 21
  val CANCELLATION_CODE = 22
  val DIVERTED = 23

  //Lookup keys
  val AIRLINE_DATA = "L_UNIQUE_CARRIERS"
  val AIRPORT_DATA = "L_AIRPORT_ID"
  val CITY_DATA = "L_CITY_MARKET_ID"

  case class FlightKey(airLineId: String, arrivalAirPortId: Int, arrivalDelay: Double)
  case class DelayedFlight(airLine: String,
                           date: String,
                           originAirport: String,
                           originCity: String,
                           destAirport: String,
                           destCity: String,
                           arrivalDelay: Double) {
  }
 
  class AirlineFlightPartitioner(partitions: Int) extends Partitioner {
    require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[FlightKey]
      k.airLineId.hashCode() % numPartitions
    }
  }
  object FlightKey {
    implicit def orderingByIdAirportIdDelay[A <: FlightKey] : Ordering[A] = {
       Ordering.by(fk => (fk.airLineId, fk.arrivalAirPortId, fk.arrivalDelay * -1))
    }
  }
  type RefTable = Table[String, String, String]
  
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("haha").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //download data from http://transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time
    //"DayofMonth","DayOfWeek","FlightDate","UniqueCarrier","AirlineID","Carrier","TailNum","FlightNum","OriginAirportID","OriginAirportSeqID","OriginCityMarketID","Origin","OriginCityName","OriginState","OriginStateFips","OriginStateName","OriginWac","DestAirportID","DestAirportSeqID","DestCityMarketID","Dest","DestCityName","DestState","DestStateFips","DestStateName","DestWac","CRSDepTime","DepTime","DepDelay","DepDelayMinutes","DepDel15","DepartureDelayGroups","DepTimeBlk","TaxiOut","WheelsOff","WheelsOn","TaxiIn","CRSArrTime","ArrTime","ArrDelay","ArrDelayMinutes","ArrDel15","ArrivalDelayGroups","ArrTimeBlk","Cancelled","CancellationCode","Diverted","CRSElapsedTime","ActualElapsedTime","AirTime","Flights","Distance","DistanceGroup","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay","FirstDepTime","TotalAddGTime","LongestAddGTime","DivAirportLandings","DivReachedDest","DivActualElapsedTime","DivArrDelay","DivDistance","Div1Airport","Div1AirportID","Div1AirportSeqID","Div1WheelsOn","Div1TotalGTime","Div1LongestGTime","Div1WheelsOff","Div1TailNum","Div2Airport","Div2AirportID","Div2AirportSeqID","Div2WheelsOn","Div2TotalGTime","Div2LongestGTime","Div2WheelsOff","Div2TailNum","Div3Airport","Div3AirportID","Div3AirportSeqID","Div3WheelsOn","Div3TotalGTime","Div3LongestGTime","Div3WheelsOff","Div3TailNum","Div4Airport","Div4AirportID","Div4AirportSeqID","Div4WheelsOn","Div4TotalGTime","Div4LongestGTime","Div4WheelsOff","Div4TailNum","Div5Airport","Div5AirportID","Div5AirportSeqID","Div5WheelsOn","Div5TotalGTime","Div5LongestGTime","Div5WheelsOff","Div5TailNum",

     val rawDataArray = sc.textFile("E:/Datasets/9112770_T_ONTIME.csv").map(line => line.split(","))
     val airlineData = rawDataArray.map(arr => createKeyValueTuple(arr))
     val keyedDataSorted = airlineData.repartitionAndSortWithinPartitions(new AirlineFlightPartitioner(1))
     keyedDataSorted.collect().foreach(println)

  }
  
  def createDelayedFlight(key: FlightKey, data: List[String], bcTable: Broadcast[RefTable]): DelayedFlight = {
    val table = bcTable.value
    val airline = table.get(AIRLINE_DATA, key.airLineId)
    val destAirport = table.get(AIRPORT_DATA, key.arrivalAirPortId.toString)
    val destCity = table.get(CITY_DATA, data(3))
    val origAirport = table.get(AIRPORT_DATA, data(1))
    val originCity = table.get(CITY_DATA, data(2))

    DelayedFlight(airline, data.head, origAirport, originCity, destAirport, destCity, key.arrivalDelay)
  }

  def createKeyValueTuple(data: Array[String]): (FlightKey, List[String]) = {
    (createKey(data), listData(data))
  }

  def createKey(data: Array[String]): FlightKey = {
    FlightKey(data(UNIQUE_CARRIER), safeInt(data(DEST_AIRPORT_ID)), safeDouble(data(ARR_DELAY)))
  }

  def listData(data: Array[String]): List[String] = {
    List(data(FL_DATE), data(ORIGIN_AIRPORT_ID), data(ORIGIN_CITY_MARKET_ID), data(DEST_CITY_MARKET_ID))
  }
  
  def safeInt(s: String): Int = {
    if (s == null || s.isEmpty || s.length == 0) 0 else s.toInt
  }

  def safeDouble(s: String): Double = {
    if (s == null || s.isEmpty || s.length == 0) 0.0 else s.toDouble
  }

}