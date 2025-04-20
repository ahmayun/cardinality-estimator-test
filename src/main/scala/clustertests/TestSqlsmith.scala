package clustertests

import org.apache.spark.sql.SparkSession
import sqlsmith.FuzzTests.{dumpSparkCatalog, gatherTargetTables, generateSqlSmithQuery, sqlSmithApi}
import sqlsmith.loader.SQLSmithLoader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util.Random

object TestSqlsmith {

  def setupSpark(master: String): SparkSession = {
    val spark = SparkSession.builder()
      .appName("Test SQLSmith")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.ui.enabled", "false")
      .master(master)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def main(args: Array[String]): Unit = {
    val master = if(args.isEmpty) "local[*]" else args(0)
    val spark = setupSpark(master)

    val numRows = 100
    val df = spark.range(numRows)
      .withColumn("random_int", (rand() * 100).cast("int"))
      .withColumn("random_str", expr("random_int").cast("string"))

    df.createOrReplaceTempView("dummy")
    spark.sql("CREATE DATABASE IF NOT EXISTS main")

    df.write
      .mode("overwrite")  // <-- key line
      .format("parquet")
      .saveAsTable("main.dummy")


    lazy val sqlSmithApi = SQLSmithLoader.loadApi()
    val targetTables = gatherTargetTables(spark)
    println("Target tables:")
    targetTables.foreach(t => println(t.name))
    println("----")
    Class.forName("org.sqlite.JDBC")
    val catalog = dumpSparkCatalog(spark, targetTables)
    val sqlSmithSchema = sqlSmithApi.schemaInit(catalog, 0)

    println(s"Test ran with ${args.mkString("Array(", ", ", ")")}")
    println(s"Successfully loaded sqlsmith JNI ${sqlSmithSchema}")


    val queryStr = generateSqlSmithQuery(sqlSmithSchema)
    println("Generated query:")
    println(queryStr)

  }
}
