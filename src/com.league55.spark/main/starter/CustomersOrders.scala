package starter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Count how much each customer spent
  */
object CustomersOrders {

  def parseLine(row: String) = {
    val parts = row.split(",") //customerId, itemId, price

    (parts(1), parts(2).toFloat)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Customers")
    val rdd = sc.textFile("src/com.league55.spark/resources/customer-orders.csv")

    val rows = rdd.map(parseLine)

    val summed = rows.reduceByKey((x, y) => x + y)

    val output = summed.map(x => (x._2, x._1)).sortByKey()
      .foreach(x => println(s"customer with id{${x._2}} spent ${x._1}%.2f "))
  }
}
