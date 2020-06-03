package dfbasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins extends App {

  // Creating Spark Session
  println(" ****************** Spark Session Initializing !!! **************************")
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // Joins
  //var guitaristsBandsDF = guitaristsDF.join(bandsDF,guitaristsDF.col("band") === bandsDF.col("id"), "inner")
  //or best way of writing
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  var guitaristsBandsDF = guitarsDF.join(bandsDF, joinCondition, "inner")

  //Outer Joins - left outer
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")

  //right outer join
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")

  //outer join - similar to full join - left join + right join
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi-join - everything in the left DF for which there is a row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left-semi")

  //anti-join - Everything in the left DF for which there is a row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")

  // Things to bear in mind
 // guitaristsDF.select("id","band").show() // this crashes

  //option1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  //option2 - drop the duplicate column
  guitaristsBandsDF.drop(bandsDF.col("id"))

  //option3 - rename the offending column & keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id","bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  //using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))





}
