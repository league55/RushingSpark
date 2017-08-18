package starter

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

/**
  * First try work with "broadcast"
  */
object PopularMovies {

  def parseLines(line: String): Int = {
    val parts = line.split("\t")
    val movieId = parts(1).toInt
    movieId
  }

  def loadMovieNames(): Map[Int, String] = {
    //encodingStaff
    implicit val codec = Codec("UTF-8")
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    codec.onMalformedInput(CodingErrorAction.REPLACE)

    val dataSrc = Source.fromFile("com.league55.spark/resources/moviesData/u.item").getLines()

    var movieNames: Map[Int, String] = Map()

    val lines = dataSrc

    for (line <- lines) {
      val fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    movieNames
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[1]", "PopularMovies")
    val rdd = sc.textFile("com.league55.spark/resources/moviesData/u.data")

    val ratedMovies = rdd.map(x => parseLines(x))

    val ratedTimes = ratedMovies.map(x => (x, 1)).reduceByKey((x, y) => x + y)

    val movieNamesById = sc.broadcast(loadMovieNames())

    val namedMovies = ratedTimes.map(m => (movieNamesById.value(m._1), m._2))

    namedMovies.map(x => (x._2, x._1)).sortByKey().foreach(println)
  }
}
