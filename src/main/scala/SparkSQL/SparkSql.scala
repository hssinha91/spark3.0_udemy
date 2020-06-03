package SparkSQL

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkSql extends App {

  // Creating Spark Session
  println(" ****************** Spark Session Initializing !!! **************************")
  val spark = SparkSession.builder()
    .appName("SParkSQL")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  def readDF(filename : String) = spark
    .read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  val carsDF = readDF("cars.json")

  //regular DF API
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  // use Spark Sql
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin
  )

  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")
 // databasesDF.show

  //transfer tables from a DB to spark tables
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm" //rtjvm is db name
  val user = "docker"
  val password = "docker"

  def readTable(tableName : String) = spark.read
    .format("jdbc")
    .options(Map(
      "driver" -> driver,
      "url" -> url,
      "user" -> user,
      "password" -> password
    )).option("dbtable", s"public.$tableName").load()

  def tranferTables(tableNames: List[String]) = tableNames.foreach{
    tablename =>
      val tableDF = readTable(tablename)
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tablename)
  }
  tranferTables(List("employees", "departments", "titles", "dept_emp", "salaries", "dept_manager"))

  //read DF from warehouse
  val employeesDF2 = spark.read.table("employees")

  /* Exercises
  * - Read the moviesDF and store it as spark table in the rtjvm db
  * - Count how many employees were hired in between Jan 1 1999 and Jan 1 2001
  * - Show the average salaries for the employees hired in between those dates, grouped by dept
  * - Show the name of the best- paying department for employees hired in between those dates  *
  * */

  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("movies")

  // 2
  spark.sql(
    """
      |select (*) from employees
      |where hire_date > '1999-01-01' and hire_date < '2001-01-01'
      |""".stripMargin
  ).show()

  // 3
  spark.sql(
    """
      |select de.dept_no, avg(s.salary) from employees e, dept_emp de, salaries s
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |and e.emp_no = de.emp_no
      |and e.emp_no = s.emp_no
      |group_by de.dept_no
      |""".stripMargin
  ).show()

  //4
  spark.sql(
    """
      |select avg(s.salary) payments, d.dept_name from employees e, dept_emp de, salaries s, departments d
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |and e.emp_no = de.emp_no
      |and e.emp_no = s.emp_no
      |and de.dept_no = d.dept_no
      |group_by de.dept_name
      |order_by payment desc
      |limit 1
      |""".stripMargin
  ).show()




}
