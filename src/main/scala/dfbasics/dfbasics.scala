package dfbasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object dfbasics extends App {

  //creating Spark Session
  println(" ****************** Spark Session Initializing !!! **************************")
  val spark = SparkSession.builder()
    .appName("Dataframe Basics")
    .config("spark.master", "local")
    .getOrCreate()

  //reading a DF
  val firstDf = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  //firstDf.show()
  //firstDf.printSchema()
  firstDf.take(10).foreach(println)

  //spark types
  val longType = LongType

  // Car Schema
  val carSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
      ))

import spark.implicits._
  val manualSmartPhoneDF = Seq(
    (2000, "Samsung", 15, 45),
    (2001, "Xiomi", 12, 25),
    (2002, "Realme", 15, 45),
    (2003, "Nokia", 13, 40),
    (2005, "Sony", 14, 55)
  )
  val smartPhonesDF = manualSmartPhoneDF.toDF("Year", "Model", "ScreenDimension", "CameraMp")

 // smartPhonesDF.show()
  //smartPhonesDF.printSchema()

  // read movies file
  val movieDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  movieDF.show()
  movieDF.printSchema()
  println("Total count of Rows = " +movieDF.count())
  //2nd way
  println(s"There are total ${movieDF.count()} in the table")
}
