package starter

import org.apache.log4j.{Level, Logger}
import org.apache.spark._

object WordsCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[1]", "WordCountBetter")
    val rdd = sc.textFile("ScalaLearning/resources/book.txt")

    val words = rdd.flatMap(_.split("\\W+"))
    val lowercaseWords = words.map(x => x.toLowerCase())

    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey((x,y) => x + y)

    val wordCountsSorted = wordCounts.map( x => (x._2, x._1)).sortByKey()

    // Print the results, flipping the (count, word) results to word: count as we go.
    for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }

  }
}
