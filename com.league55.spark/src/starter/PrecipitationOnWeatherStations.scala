package starter

import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}

case class PRCP(value: Float, date: Int) {
  def bigger(that: PRCP): PRCP = {
    if(this.value > that.value) this else that
  }
}

/**
  * Find the most Precipitation on each weather station in 1800
  **/
object Precipitation {

  def parseLine(string: String) = {
    val splitted = string.split(",")

    val stationId = splitted(0)
    val operation = splitted(2)
    val value = splitted(3).toFloat
    val date = splitted(1).toInt

    (stationId, operation, value, date)
  }

  private val PRECIPITATION: String = "PRCP"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Precipitation")

    val rdd = sc.textFile("com.league55.spark/resources/1800.csv")

    val parsedData = rdd.map(parseLine)                             //(stationId, type, temp)

    val prcpRows = parsedData.filter(x => x._2 == PRECIPITATION)    //filter out only Precipitation rows
    val prcpData = prcpRows.map(x => (x._1, PRCP(x._3, x._4)))  //(stationId, (value, date))

    val minPrcpByStation = prcpData.reduceByKey((x,y) => x.bigger(y)) //find the most one on each station

    val results = minPrcpByStation.collect().sortBy(x=>x._1).foreach(println)

  }
}
