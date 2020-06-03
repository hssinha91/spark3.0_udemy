package sparktypes_and_datasets

import java.sql.Date

import org.apache.spark.sql.{SparkSession, Encoders, Dataset}
import org.apache.spark.sql.functions._

object Datasets extends App {

  //creating Spark Session
  println(" **********************************************************")
  println("           Spark Session Initializing !!!                  ")
  println(" **********************************************************")
  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark
    .read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int] // this works fine for single column value
  //numbersDS.show()
  // For more than one column, we use case class
  // dataset for complex type
  // step 1 - define your case class
  import spark.implicits._
  case class Cars( Name : String,
                   Miles_per_Gallon : Option[Double],
                   Cylinders : Long,
                   Displacement : Double,
                   Horsepower : Option[Long],
                   Weight_in_lbs : Long,
                   Acceleration : Double,
                   Year : String,
                   Origin : String
                 )

  // Step 2 - read the DF from the file
  def readDF(filename : String) = spark
    .read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  // step 3 - define your Encoder(importing the implicits)

  val carsDF = readDF("cars.json")
  val carsDS = carsDF.as[Cars] //convert DF to DS

//carsDS.show()
  carsDF.printSchema()
  // DS collection Function
  //numbersDS.filter(_ < 100)

  var carNamesDS = carsDS.map(nm => nm.Name.toUpperCase())
  carsDS.map(_.Name.toUpperCase()) //alternate to above

  /* Exercise
  * Count how many cars we have
  * Count how many powerful cars we have(HP > 140)
  * Average HP for the entire dataset
  * */

  // 1 - Count how many cars we have
  println(carsDS.count())

  // 2 -  Count how many powerful cars we have(HP > 140)
  carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count

  // 3 - Average HP for the entire dataset
  val carsCount = carsDS.count
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _)/carsCount)
  carsDS.select(avg(col("Horsepower"))).show() // using DF

  // Joins
  case class Guitar(
                   id: Long,
                   make: String,
                   model: String,
                   guitarType: String
                   )

  case class GuitarPlayer(
                         id: Long,
                         name: String,
                         guitars: Seq[Int],
                         band: Int
                         )

  case class Band(
                 id: Long,
                 name: String,
                 hometown: String,
                 year: Int
                 )

  val guitarDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarsPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS = guitarPlayersDS
    .joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")
  guitarPlayerBandsDS.show()

  /* Exercise
  * Join guitar DS & guitarPlayer DS , in an outer join. hint - use array_contains
  * */

  guitarPlayersDS.joinWith(guitarDS, array_contains(guitarPlayersDS.col("guitars"), guitarDS.col("id")), "outer").show()

  // Grouping DS
  val carsGroupedByOrigin = carsDS
    .groupByKey(_.Origin)
    .count()
    .show()
  //note - join and groups are wide transformations, will involve shuffle operations

}
