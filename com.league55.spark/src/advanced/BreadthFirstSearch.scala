package advanced

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer
import scala.math.min

/**
  * Own implementation of breadth-first-search made in studing purposes,
  * based on code from lectures.
  * Searching for degrees of separation of superheroes
  */

object BreadthFirstSearch {
  val startingHeroId = 5312
  val targetHeroId = 6196

  var hitCounter: Option[LongAccumulator] = None

  val WHITE = "WHITE" //not-studied
  val GRAY = "GRAY" //going to be studied
  val BLACK = "BLACK" //studied
  val INFINITY = 999999 // "infinity" - default distance, just means "big"

  type bfsNode = (Int, bfsData) //heroId, data
  type bfsData = (Array[Int], Int, String) //connections, distance, color
  val sc = new SparkContext("local[*]", "BFS")

  def getInitialData(sc: SparkContext): RDD[bfsNode] = {
    val rdd = sc.textFile("com.league55.spark/resources/Marvel-graph.txt")
    rdd.map(convertToBfsNode)
  }

  def convertToBfsNode(line: String): bfsNode = {
    val parts = line.split("\\s+")
    val heroId = parts(0).toInt
    val connections: ArrayBuffer[Int] = new ArrayBuffer()

    for (connection <- 1 until parts.length) {
      connections += parts(connection).toInt
    }
    val isHeroStartingHero = heroId == startingHeroId

    val color = if (isHeroStartingHero) GRAY else WHITE
    val distance = if (isHeroStartingHero) 0 else INFINITY

    val data = new bfsData(connections.toArray, distance, color)
    new bfsNode(heroId, data)
  }

  def getDarkestColor(color1: String, color2: String): String = {
    if (color1 == WHITE && (color2 == GRAY || color2 == BLACK)) return color2
    if (color1 == GRAY && color2 == BLACK) return color2
    if (color2 == WHITE && (color1 == GRAY || color1 == BLACK)) return color1
    if (color2 == GRAY && color1 == BLACK) return color1

    WHITE
  }

  //flatMaps 1 bfsNode to array unwrapping all connections to their own bfsNodes
  def flatMapBfsNodes(node: bfsNode): Array[bfsNode] = {
    val result: ArrayBuffer[bfsNode] = new ArrayBuffer[(Int, (Array[Int], Int, String))]()

    val heroId = node._1
    val data = node._2

    val connections = data._1
    val distance = data._2
    var color = data._3

    // GRAY nodes are to be checked during this iteration
    if (color == GRAY) {
      for (id <- connections) {
        if (id == targetHeroId) {
          if (hitCounter.isDefined) {
            hitCounter.get.add(1)
          }
        }

        val newEntry: bfsNode = (id, (Array(), distance + 1, GRAY))
        result += newEntry
      }
    }
    //this node is studied
    color = BLACK

    //don't loose this node too
    val thisEntry: bfsNode = (heroId, (connections, distance, color))
    result += thisEntry

    result.toArray
  }

  //reduce choosing less distance and dark color merging connections
  def reduceBfsNodes(data1: bfsData, data2: bfsData): bfsData = {
    val connections1 = data1._1
    val connections2 = data2._1

    val distance1 = data1._2
    val distance2 = data2._2

    val color1 = data1._3
    val color2 = data2._3
    // Default node values
    var distance:Int = INFINITY
    var color:String = WHITE
    var connections: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    if (connections1.length > 0) {
      connections ++= connections1
    }
    if (connections2.length > 0) {
      connections ++= connections2
    }

    distance = min(distance1, distance2)
    color = getDarkestColor(color1, color2)

    (connections.toArray, distance, color)
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var rdd = getInitialData(sc)

    //signal that target found
    hitCounter = Some(sc.longAccumulator("Hit Counter"))

    var iteration = 0

    for (iteration <- 1 to 10) {
      println("Running BFS Iteration# " + iteration)
      val mapped = rdd.flatMap(flatMapBfsNodes)
      println("Processing " + mapped.count() + " values.") //force RDD to accumulate

      if (hitCounter.isDefined) {
        val hitCount = hitCounter.get.value
        if (hitCount > 0) {
          println("Hit the target character! From " + hitCount +
            " different direction(s).")
          return
        }
      }

      // Reducer combines data for each character ID, preserving the darkest
      // color and shortest path.
      rdd = mapped.reduceByKey(reduceBfsNodes)
    }

  }
}
