import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import fuzzer.global.State.sparkOption

object FuzzerGeneratedProgramHarness {

  def main(args: Array[String]): Unit = {
    val spark = sparkOption.get

    val auto5 = spark.table("tpcds.store_sales")
    val auto0 = spark.table("tpcds.date_dim")
    val auto6 = auto5.withColumn("RqFuw", col("store_sales.ss_quantity") > 5)
    val auto7 = auto6.withColumn("f3gEt", col("store_sales.ss_coupon_amt") > 5)
    val auto3 = auto7.join(auto0, col("date_dim.d_qoy") === col("store_sales.ss_quantity"), "inner")
    val auto4 = auto3.withColumn("eVJXP", col("date_dim.d_dom") > 5)
    val auto8 = auto5.join(auto4, col("date_dim.d_first_dom") === col("store_sales.ss_cdemo_sk"), "right")
    val auto9 = auto8.withColumn("irUW2", col("date_dim.d_fy_week_seq") > 5)
    val auto1 = auto9.join(auto0, col("date_dim.d_dom") === col("store_sales.ss_hdemo_sk"), "right")
    val auto2 = auto1.as("f1veg")
    val sink = auto6.join(auto2, col("f1veg.ss_sold_time_sk") === col("f1veg.d_fy_year"), "outer")
    sink.explain(true)

    fuzzer.global.State.finalDF = Some(sink)
  }
}