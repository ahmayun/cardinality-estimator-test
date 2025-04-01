package clustertests

import org.apache.spark.sql.SparkSession

object TestSingleQuery {
  def main(args: Array[String]): Unit = {
    val master = if(args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("TestSingleQueryRaw")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val q = """
              |select
              |  ref_0.ws_promo_sk as c0,
              |  ref_0.ws_order_number as c1,
              |  subq_0.c0 as c2,
              |  subq_0.c0 as c3,
              |  subq_0.c0 as c4,
              |  ref_0.ws_item_sk as c5,
              |  subq_0.c0 as c6
              |from
              |  main.web_sales as ref_0,
              |  lateral (select
              |        ref_1.ca_street_name as c0,
              |        ref_1.ca_city as c1,
              |        ref_1.ca_address_sk as c2,
              |        ref_0.ws_net_paid_inc_ship as c3
              |      from
              |        main.customer_address as ref_1
              |      where ref_1.ca_zip is NULL) as subq_0
              |where (subq_0.c2 is NULL)
              |  or (ref_0.ws_list_price is not NULL)
              |limit 81
              |""".stripMargin

    val st = System.nanoTime()
    val df = spark.sql(q)
    df.show(4)
    val et = System.nanoTime()

    println(s"Total time (s): ${(et-st) / 1e9}")

  }
}
