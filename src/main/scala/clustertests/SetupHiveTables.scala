package clustertests

import org.apache.spark.sql.SparkSession

object SetupHiveTables {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HiveCheck")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("CREATE DATABASE IF NOT EXISTS main")
    spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS call_center STORED AS PARQUET LOCATION 'hdfs://tpcds/call_center';")

  }

}
