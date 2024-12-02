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
              |  ref_0.cs_bill_cdemo_sk as c0,
              |  ref_0.cs_bill_cdemo_sk as c1
              |from
              |  main.catalog_sales as ref_0
              |where cast(coalesce(ref_0.cs_order_number,
              |    ref_0.cs_ship_addr_sk) as INTEGER) is NULL
              |limit 68
              |""".stripMargin

    val st = System.nanoTime()
    withOptimized {
      spark.sql(q).show(5)
    }
    val et = System.nanoTime()
    println(s"Total time (s): ${(et-st) / 1e9}")

  }
}
