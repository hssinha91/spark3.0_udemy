package sparktypes_and_datasets

import org.apache.spark.sql.SparkSession
import  org.apache.spark.sql.functions._

object CommonTypes extends App {

  //creating Spark Session
  println(" ****************** Spark Session Initializing !!! **************************")
  val spark = SparkSession.builder()
    .appName("Common Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_Value"))

  //Boolean
  val dramaFilter = col("Major_Genre") === "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  moviesDF.select("Title").where(dramaFilter)

  // + multiple ways of filtering
  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movies"))

  //filter on a boolean column
  moviesWithGoodnessFlagsDF.where("good_movies").show() //where (col("good_movies") === "true")

  //negations
  moviesWithGoodnessFlagsDF.where(not(col("good_movies")))

  //Numbers
  /* Math Operator */
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_RAting")) / 2)

  //Correlation = number between -1 & 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) /*Corr is anAction*/

  //Strings
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  //capitalization - initcap, upper, lower
  carsDF.select(initcap(col("Name")), upper(col("Name")), lower(col("Origin")))

  //contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // more powerful way of using contains in regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("Regex_Extract"))
    .where(col("Regex_Extract") =!= "")

  carsDF.select(col("Name"),
    regexp_replace(col("Name"), regexString, "Peoples_Car").as("Regex_Replace")
  )

  /* Exercise -
  - Filter the cars DF by a list of cars names obtained by an API Call
  - version - contains , regex
   */

  def getCarNames : List[String] = List("Volkswagen", "Mercedes_Benz", "Ford")

  //version 1
  val complexRegex = getCarNames.map(_.toLowerCase()).mkString("|") //volkswagen|mercedes_benz|ford
  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("Regex_Extract")
  ).drop("Regex_Extract")

 //version2 - contains
  val carNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false)) ((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)

  carsDF.filter(bigFilter).show()

}
