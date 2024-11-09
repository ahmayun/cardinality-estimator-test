package misc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf


object SingleQueryRunner {
  def main(args: Array[String]): Unit = {
    // TODO: Makes this code independent from the Spark version
    val spark = SparkSession.builder()
      .appName("FuzzTest")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

//    def myScalaFunction(s: String): String = {
//      s.toUpperCase // Example function that converts input string to uppercase
//    }
//
//
//    val myUDF = udf(myScalaFunction _)
//    spark.udf.register("myScalaFunction", myUDF)
//    spark.sql("SELECT myScalaFunction(cc_mkt_class) FROM main.customer_demographics")

    val q1 = spark.sql(
      """
        |select
        |  12 as c0
        |from
        |  main.customer_demographics as ref_0
        |where ref_0.cd_credit_rating is NULL
        |""".stripMargin)

    q1.explain("cost")
    println(s"Estimated count: ${q1.queryExecution.optimizedPlan.stats.rowCount}")
    println(s"Actual count: ${q1.count()}")

    val q2 = spark.sql(
      """
        |select
        |  ref_0.cc_mkt_class as c0
        |from
        |  main.call_center as ref_0
        |where ref_0.cc_division_name is NULL
        |limit 95
        |""".stripMargin)

    q2.explain("cost")
    println(s"Estimated count: ${q2.queryExecution.optimizedPlan.stats.rowCount}")
    println(s"Actual count: ${q2.count()}")
  }
}
