package sparktypes_and_datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App {

  //creating Spark Session
  println(" **********************************************************")
  println("           Spark Session Initializing !!!                  ")
  println(" **********************************************************")
  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //select the first non-null value
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
  )

  //checking for Nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  //Nulls when ordering
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last) //desc, desc_nulls_first

  //removing Nulls
  moviesDF.select("Title", "IMDB_Rating").na.drop() // remove rows containing nulls

  //replace Nulls
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
  moviesDF.select("Director","IMDB_Rating", "Rotten_Tomatoes_Rating").na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  ))

  //complex operations
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifNull(Rotten_Tomatoes_Rating, IMDB_Rating*10)", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating*10)", // same as coalesce
    "nullIf(Rotten_Tomatoes_Rating, IMDB_Rating*10)", //returns null if the two values are Equal, else first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating*10, 0.0)" // same as - if(first !=null) then second else third
  ).show

}
