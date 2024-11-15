package clustertests

import org.apache.spark.sql.SparkSession
import sqlsmith.loader.SQLSmithLoader

object Test {

  def main(args: Array[String]): Unit = {
    lazy val sqlSmithApi = SQLSmithLoader.loadApi()
    val sqlSmithSchema = sqlSmithApi.schemaInit("", 0)

    println(s"Test ran with ${args.mkString("Array(", ", ", ")")}")
    println(s"Successfully loaded sqlsmith JNI ${sqlSmithSchema}")

    println("Attempting to create external table to check hive setup...")

    val spark = SparkSession.builder()
      .appName("HiveCheck")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master(args(0))
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.sql("select * from main.customer")
    df.show(5)
    println(s"total rows: ${df.count()}")

  }
}
