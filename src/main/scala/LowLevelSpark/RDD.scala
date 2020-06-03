package LowLevelSpark

import scala.io.Source
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object RDD extends App {

  // Creating Spark Session
  println(" ****************** Spark Session Initializing !!! **************************")
  val spark = SparkSession.builder()
    .appName("SParkSQL")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  // Ways to create RDD
  // 1 - parallelize an existing collection
  val numbers = 1 to 100
  val numbersRDD = sc.parallelize(numbers)

  // 2a - reading from files
  case class StockValue(company: String, date: String, price: Double)

  def readStocks(filename: String) = Source.fromFile(filename)
    .getLines()
    .drop(1)
    .map(line => line.split(","))
    .map((tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble)))
        .toList
  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  //2b - reading from textfile
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - Reading a DF
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("numbers") // we will lose the type info

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD) // we get to keep type info

  // Transformations
  // counting
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") //lazy transformation
  val msCount = msftRDD.count()

  // distinct
  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() //also lazy

  // min and max
  implicit val stockOrdering: Ordering[StockValue] = Ordering
    .fromLessThan[StockValue]((sa:StockValue, sb: StockValue) => sa.price < sb.price)
  val numMsft = msftRDD.min() //Action

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol) // very expensive

  // partitioning
  val repartitionedStocksRDD = stocksRDD
    .repartition(30)
    repartitionedStocksRDD.toDF().write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")

  /* Repartitioning is Expensive. Involves shuffling
  * Best Practice -> Partition early, then process that
  * Size of partition - 10 - 100MB
  * */

  // Coalesce
  val coalescedRDD = repartitionedStocksRDD.coalesce(15) //doesnot involve shuffling
  coalescedRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")

  /* Exercise
  * - Read the movies.json as an RDD
  * - Show the distinct genres as an RDD
  * - Select all the movies in the Drama genre with the IMDB rating > 6
  * - Show the avg rating of the movie by genre
  * */

  case class Movie(title: String, genre: String, rating: Double)

  // 1 -
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesRDD = moviesDF.select(
    col("Title").as("title"),
    col("Major_Genre").as("genre"),
    col("IMDB_").as("rating"))
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  // 2 -
  val genreRDD = moviesRDD.map(_.genre).distinct()

  // 3 -
  val goodDramasRDD = moviesRDD.filter(
    movie => movie.genre == "Drama" && movie.rating > 6
  )
  moviesRDD.toDF.show()
  genreRDD.toDF.show()
  goodDramasRDD.toDF.show()

  // 4 -
  case class GenreAvgRating(genre: String, rating: Double)
  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map{
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
  }
  avgRatingByGenreRDD.toDF.show()

}
