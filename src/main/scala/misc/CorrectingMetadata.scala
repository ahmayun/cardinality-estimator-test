package misc

import org.apache.spark.sql.SparkSession
import sqlsmith.FuzzerArguments

import java.io.File

object CorrectingMetadata {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CorrectingMetadata")
      //      .config("spark.sql.cbo.enabled", "true")
      //      .config("spark.sql.cbo.joinReorder.enabled", "true")
      //      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      //      .config("spark.sql.statistics.histogram.enabled", "true")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val targetTables = {
      val tables = spark.catalog.listTables()
      tables.collect()
    }
//
    println(targetTables.mkString("Array(", ", ", ")"))
//    println(spark.catalog.listCatalogs().collect().mkString("Array(", ", ", ")"))
    println(spark.catalog.listTables().collect().mkString("Array(", ", ", ")"))
    //    spark.sql("""create external table people.people stored as parquet location "/home/ahmad/Documents/project/cardinality-estimator-test/spark-warehouse" """)

//    val df = spark.read.parquet("/home/ahmad/Documents/project/cardinality-estimator-test/spark-warehouse")
//    df.write.mode("overwrite").saveAsTable("person.person")
//    spark.sql("show tables in person").show()
  }

}
