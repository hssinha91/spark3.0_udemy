package dfbasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ColumnsandExpressions extends App {

  //creating Spark Session
  println(" ****************** Spark Session Initializing !!! **************************")
  val spark = SparkSession.builder()
    .appName("Columns And Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read.option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Columns
  val firstColumn = carsDF.col("Name")

  // selecting column
  val carNamesDF = carsDF.select(firstColumn)
  //carNamesDF.show()
// selecting methods
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year,  // scala symbol, auto convert to column
    $"HorsePower", // fancier interpolated string, returns a col. object
    expr("origin") // Expression
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  //Expressions
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )
  //carsWithWeightsDF.show()

  //select Expr
  var carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name", "Weight_in_lbs", "Weight_in_lbs / 2.2"
  )
  //carsWithSelectExprWeightsDF.show()

  // Dataframe processing
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs")/2.2)

  //Renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")

  //careful with column names - calling renamed column from above in select
  carsWithColumnRenamed.selectExpr("`Weight_in_pounds`")

  //remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  //Filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")

  //Filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
 // americanCarsDF.show()

  //chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  //unioning = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF)
  //allCarsDF.show()

  //distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  //allCountriesDF.show()

  /* Exercises -
  * - Read the movies DF and select 2 columns of your choice
  * - create another column suming the total profit of the movies = us_gross + worldwide_gross +
  * - select all comedy movies with IMDB rating above 6.
  */
  println(" ************************** Exercise Answer ************************ ")
  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  val selectTwoColumns = moviesDF.select(
    col("Title"),
    col("Release_Date")
  )
  //selectTwoColumns.show()

  val moviesTotalProfit = moviesDF.select(
    col("Title"),
    col("Release_Date"),
    expr("US_Gross + Worldwide_Gross").as("Total_Profit")
  ).show()

  val goodComedyMovies = moviesDF.filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6).show()

}
