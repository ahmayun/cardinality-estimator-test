package clustertests

import org.apache.spark.sql.SparkSession
import sqlsmith.FuzzTests.withOptimized

object TestSingleQueryOpt {
  def main(args: Array[String]): Unit = {
    val master = if(args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("TestSingleQueryOpt")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val q = """
      |select
      |  subq_0.c0 as c0,
      |  subq_0.c0 as c1,
      |  ref_0.c_customer_sk as c2,
      |  subq_0.c0 as c3,
      |  79 as c4,
      |  subq_0.c0 as c5,
      |  ref_0.c_customer_id as c6
      |from
      |  main.customer as ref_0,
      |  lateral (select
      |        ref_1.wr_order_number as c0
      |      from
      |        main.web_returns as ref_1
      |      where ref_1.wr_account_credit is not NULL
      |      limit 114) as subq_0
      |where ref_0.c_first_name is NULL
      |limit 69
      |""".stripMargin

    val st = System.nanoTime()
    withOptimized {
      spark.sql(q).show(5)
    }
    val et = System.nanoTime()
    println(s"Total time (s): ${(et-st) / 1e9}")

  }
}
