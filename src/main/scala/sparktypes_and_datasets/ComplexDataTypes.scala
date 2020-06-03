package sparktypes_and_datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexDataTypes extends App {

  //creating Spark Session
  println(" ****************** Spark Session Initializing !!! **************************")
  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates
  moviesDF.select(col("Title"),col("Release_Date"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release")) //conversion
    .withColumn("Today", current_date()) //today
    .withColumn("Right_Now", current_timestamp()) // this second
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365)// dateadd., datesub

  val moviesWithReleaseDate = moviesDF.select(
    col("Title"),
    to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release")
  )

  moviesWithReleaseDate.select("*").where(col("Actual_Release").isNull)

  /* Exercise -
  - How to do we deal with multiple date formats?
  - Read the stocks DF and parse the dates?
   */

  // 1 - parse the df multiple times then union the small df's

  // 2 -
  val stocksDF = spark.read
    .format("csv")
    .options(Map(
      "inferSchema" -> "true",
      "header" -> "true"
    )).load("src/main/resources/data/stocks.csv")

  val stocksDFWithDates = stocksDF
    .withColumn("Actual_Date", to_date(col("date"), "MMM dd yyyy"))

  /* Structures - are group of columns aggregated  into one.
  * Structures are noting but tuples from a column that is composed of multiple values
  * */
  // version 1 - with col operators

  moviesDF.select(
    col("Title"),
    struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"),
      col("Profit").getField("US_Gross").as("US_Profit"))

  //version 2 - with expression strings
  moviesDF.selectExpr(
    "Title",
    "(US_Gross, Worldwide_Gross) as Profit"
  ).selectExpr("Title", "Profit.US_Gross")

  //Arrays
  val moviesWithWords = moviesDF.select(
    col("Title"),
    split(col("Title"), " |,").as("Title_Words")) // Array of Strings

  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "love")
  ).show()

}
