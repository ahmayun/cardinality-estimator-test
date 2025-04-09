package misc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FuzzerGeneratedProgramHarness {

  def main(args: Array[String]): Unit = {
    val master = if(args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("Fuzzer Generated Program")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val auto0 = spark.table("tpcds.household_demographics")
    val auto6 = spark.table("tpcds.warehouse")
    val auto4 = auto0.as("IcuBp")
    val auto5 = auto4.as("VixhC")
    val auto7 = auto6.join(auto5, col("warehouse.w_country") === col("VixhC.hd_buy_potential"), "inner")
    val auto8 = auto7.withColumn("ioatd", length(col("warehouse.w_street_name")) > 5)
    val auto1 = auto8.join(auto0, col("warehouse.w_warehouse_sq_ft") === col("VixhC.hd_dep_count"), "right")
    val auto3 = auto1.withColumn("vMWuU", length(col("warehouse.w_warehouse_name")) > 5)
    val auto9 = auto6.join(auto3, col("warehouse.w_zip") === col("VixhC.hd_buy_potential"), "inner")
    val auto10 = auto9.as("jCYyX")
    auto10.explain(true)

  }
}

