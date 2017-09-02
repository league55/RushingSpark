package advanced

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}
import scala.math.sqrt

/**
  * Searching for movies with similar ratings from same users
  * supposed to be started via console
  */
object SimilarMovies {

  def loadMoviesNames(): Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var moviesNames: Map[Int, String] = Map()
        val lines = Source.fromFile("../resources/moviesData/u.item").getLines()
//    val lines = Source.fromFile("src/com.league55.spark/resources/moviesData/u.item").getLines()

    for (line <- lines) {
      val fields = line.split('|')
      if (fields.length > 1) {
        moviesNames += (fields(0).toInt -> fields(1))
//        println(fields(1))
      }
    }
    moviesNames
  }

  def loadGenres(): Map[Int, Array[Int]] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var moviesNames: Map[Int, Array[Int]] = Map()
        val lines = Source.fromFile("../resources/moviesData/u.item").getLines()
//    val lines = Source.fromFile("src/com.league55.spark/resources/moviesData/u.item").getLines()

    for (line <- lines) {
      val fields = line.split('|')
      if (fields.length > 1) {
        moviesNames += (fields(0).toInt -> fields.takeRight(19).map(_.toInt))
      }
    }
    moviesNames
  }

  type Rating = (Int, Double)
  type TwoRatings = (Int, (Rating, Rating))

  def filterDuplicates(moviesPair: TwoRatings): Boolean = {
    val rating1 = moviesPair._2._1
    val rating2 = moviesPair._2._2

    //imagine movies x and y, this will filer out pairs (x, x), (y, y), (x < y) and left only 1 occurrence (x > y)
    rating1._1 > rating2._1
  }

  def makePairs(rates: TwoRatings) = {
    val rating1 = rates._2._1
    val rating2 = rates._2._2

    val movieId1 = rating1._1
    val movieId2 = rating2._1

    val score1 = rating1._2
    val score2 = rating2._2

    ((movieId1, movieId2), (score1, score2))
  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs: RatingPairs): (Double, Int) = {
    var numPairs: Int = 0
    var sum_xx: Double = 0.0
    var sum_yy: Double = 0.0
    var sum_xy: Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator: Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)

    var score: Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    return (score, numPairs)
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "SimilarMovies")

    val nameDict = loadMoviesNames()
    val data = sc.textFile("../resources/moviesData/u.data")

    val rates = data.map(_.split("\\t")).map(x => (x(0).toInt, (x(1).toInt, x(2).toDouble)))
    //take into account only ratings > 3
    val goodRates = rates.filter(_._2._2 > 3)
    //join ratings with themself to get all combination and then filter out duplicates like
    val joinedRates = rates.join(rates).filter(filterDuplicates)
    //(movie1, movie2) => (rating1, rating2)
    val moviePairs = joinedRates.map(makePairs)
    //(movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
    val grouped = moviePairs.groupByKey()

    val moviePairSimilarities = grouped.mapValues(computeCosineSimilarity)
    //boost movies with the same genre

    if (args.length > 0) {
      val scoreThreshold = 0.97
      val coOccurenceThreshold = 50.0

      val movieID: Int = args(0).toInt

      // Filter for movies with this sim that are "good" as defined by
      // our quality thresholds above

      val filteredResults = moviePairSimilarities.filter(x => {
        val pair = x._1
        val sim = x._2
        (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
      }
      )

      // Sort by quality score.
      val results = filteredResults.map(x => (x._2, x._1)).sortByKey(false).take(10)

      println(s"there are ${nameDict.size} movies ")
      println("\nTop 10 similar movies for " + nameDict(movieID))
      for (result <- results) {
        val sim = result._1
        val pair = result._2
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = pair._1
        if (similarMovieID == movieID) {
          similarMovieID = pair._2
        }
        println(nameDict(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
    }
  }
}

