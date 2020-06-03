package dfbasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  //creating Spark Session
  println(" ****************** Spark Session Initializing !!! **************************")
  val spark = SparkSession.builder()
    .appName("Aggregations")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) //all values except null
  moviesDF.selectExpr("count(Major_Genre)")

  //counting all
  moviesDF.select(count("*")) //all records including null

  //counting distinct
  moviesDF.select(countDistinct("Major_Genre"))
  moviesDF.select(countDistinct(col("Major_Genre")))

  //approximate count
  moviesDF.select(approx_count_distinct("Major_Genre"))
  moviesDF.select(approx_count_distinct(col("Major_Genre")))

  //min and max
  moviesDF.select(min("IMDB_Rating"))
  moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  //sum
  moviesDF.select(sum("US_Gross"))
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")

  //avg
  moviesDF.select(avg("Rotten_Tomatoes_Rating"))
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  //data science - mean and stddev
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )

  //grouping
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre")) // including null
    .count()

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    ).orderBy(col("Avg_Rating"))

  /* Exercise
  * - Sum up all the profits of all the movies in the DF
  * - Count how many distinct directors we have
  * - Show the mean and standard deviation of US_Gross revenue for the movies
  * - Compute the avg IMDB_Rating & the avg US_Gross revenue per director
  * */

  val sumMoviesDF = moviesDF.select(
    (col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross")
  ).select(sum("Total_Gross")).show()

  val distinctDirectorsDF = moviesDF.select(countDistinct("Director")).show()

  val meanRevenueMoviesDF = moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross")
  )

  val avgRevenuePerDirector = moviesDF.select("Director","IMDB_Rating", "US_Gross")
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Imdb_Rating"),
      avg("US_Gross").as("Avg_Us_Gross")
    ).orderBy(col("Avg_Imdb_Rating").desc_nulls_last).show()














}
