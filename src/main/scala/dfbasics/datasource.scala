package dfbasics

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object datasource extends App {

  //creating Spark Session
  println(" ****************** Spark Session Initializing !!! **************************")
  val spark = SparkSession.builder()
    .appName("Dataframe Basics")
    .config("spark.master", "local")
    .getOrCreate()

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

  val carsDF = spark.read
    .format("json")
    .schema(carSchema)
    .option("mode", "failFast") // dropMalFormed, permissive
    //   .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  //Alternative reading with options map
  val carsSchemaWithOptionMap = spark.read
    .format("json")
    //.schema(carSchema)
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()
  carsSchemaWithOptionMap.show()

  /*
  writing a DFs
  - format
  - save mode = overwrite, append, ignore, errorIfExists
  - path
  - zero or more options
  */

  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite) //anotherway - mode("overwrite") but savemode.overwrite is best way
    .save("src/main/resources/data/cars_dup.json")

  // json flags
  spark.read.schema(carSchema)
    .options(Map(
      "dateFormat" -> "YYYY-MM-dd",
      "allowSinglequotes" -> "true",
      "compression" -> "uncompressed", // others - bzip2, gzip, lz4, snappy, deflate
    )).json("src/main/resources/data/cars.json")

  // cvs flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read.schema(stocksSchema).options(Map(
    "dateFormat" -> "MMM dd YYYY",
    "header" -> "true",
    "sep" -> ",",
    "nullValue" -> ""
  )).csv("src/main/resources/data/stocks.csv")

  // write in parquet
  /* default saves in parquet so no need to provide format */
  carsDF.write.mode(SaveMode.Overwrite).save("src/main/resources/data/cars.csv.parquet")

  // read text files
  spark.read.text("").show()

  // reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm" //rtjvm is db name
  val user = "docker"
  val password = "docker"
  val employeeDF = spark.read.format("jdbc")
    .options(Map(
      "driver" -> driver,
      "url" -> url,
      "user" -> user,
      "password" -> password
    )).option("dbtable", "public.employees").load()

  /* Exercise: read the movies DF then write as
  * - tab-separated values file
  * - snappy parquet
  * - table "public.movies" in the postgresql
  * */
  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // csv
  moviesDF.write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/data/movies.csv")

  // parquet
  moviesDF.write.save("src/main/resources/data/movies.parquet")

  // writing to DB
  moviesDF.write
    .format("jdbc")
    .options(Map(
      "driver" -> driver,
      "url" -> url,
      "user" -> user,
      "password" -> password
    )).option("dbtable", "public.movies").save()
}

