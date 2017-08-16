package starter

import org.apache.log4j._
import org.apache.spark._

/**
  * Created by maksym on 13.08.17.
  */
object FriendsByAge {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val name = fields(1).toString
    val numFriends = fields(3).toInt

    (name, numFriends)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByAgeOwn")

    val lines = sc.textFile("ScalaLearning/resources/fakefriends.csv")

    val rdd = lines.map(parseLine)  //(33, 10)

    val totalsByAge = rdd.groupByKey()
                          .mapValues(v => (v.sum, v.size))   //(33, (10, 1))

    val result = totalsByAge.mapValues(x => x._1 / x._2)

    val sortedResult = result.collect().sortBy((x)=> x._2).foreach(println)

  }

}
