package clustertests

import org.apache.spark.sql.SparkSession
import sqlsmith.FuzzTests.metricComputers


object TestEstimator {
  def main(args: Array[String]): Unit = {
    val master = if(args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("TestEstimator")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master(master)
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

    val q1 ="""
        |select
        |  ref_0.ss_ticket_number as c0,
        |  ref_0.ss_net_profit as c1,
        |  ref_0.ss_store_sk as c2,
        |  ref_0.ss_list_price as c3,
        |  ref_0.ss_net_profit as c4,
        |  ref_0.ss_net_paid as c5,
        |  ref_0.ss_sold_time_sk as c6,
        |  ref_0.ss_ext_tax as c7,
        |  ref_0.ss_ticket_number as c8,
        |  ref_0.ss_net_paid_inc_tax as c9,
        |  ref_0.ss_ticket_number as c10,
        |  ref_0.ss_promo_sk as c11,
        |  (select sm_ship_mode_sk from main.ship_mode limit 1 offset 1)
        |     as c12,
        |  ref_0.ss_net_paid_inc_tax as c13,
        |  ref_0.ss_sales_price as c14,
        |  (select (inv_date_sk + inv_item_sk) as c20 from main.inventory limit 1 offset 4)
        |     as c15,
        |  ref_0.ss_wholesale_cost as c16,
        |  ref_0.ss_net_paid_inc_tax as c17,
        |  ref_0.ss_hdemo_sk as c18
        |from
        |  main.store_sales as ref_0
        |where ref_0.ss_cdemo_sk is NULL
        |limit 100
        |""".stripMargin
    val df1 = spark.sql(q1)

    df1.explain("cost")
    println(s"Estimated count: ${df1.queryExecution.optimizedPlan.stats.rowCount}")
    println(s"Actual count: ${df1.count()}")

    metricComputers.foreach {f =>
      val (name, value) = f(q1)
      val s = s"$name:\n$value"
      println(s)
      s
    }
  }
}
