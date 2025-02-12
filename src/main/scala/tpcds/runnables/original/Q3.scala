package tpcds.runnables.original

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.RuleExecutor

object Q3 {


  def main(args: Array[String]): Unit = {
    val master = if(args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("TPC-DS Q3")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val q = """
              | SELECT dt.d_year, tpcds.item.i_brand_id brand_id, tpcds.item.i_brand brand,SUM(ss_ext_sales_price) sum_agg
              | FROM  tpcds.date_dim dt, tpcds.store_sales, tpcds.item
              | WHERE dt.d_date_sk = tpcds.store_sales.ss_sold_date_sk
              |   AND tpcds.store_sales.ss_item_sk = tpcds.item.i_item_sk
              |   AND tpcds.item.i_manufact_id = 128
              |   AND dt.d_moy=11
              | GROUP BY dt.d_year, tpcds.item.i_brand, tpcds.item.i_brand_id
              | ORDER BY dt.d_year, sum_agg desc, brand_id
              | LIMIT 100
              |""".stripMargin

    spark.sql(q).explain(true)
    
    val st = System.nanoTime()
    spark.sql(q).show(5)
    val et = System.nanoTime()

    println(s"Total time (s): ${(et-st) / 1e9}")
    println(RuleExecutor.dumpTimeSpent().split("\n").filter(!_.contains("0 / ")).mkString("\n"))
  }

}
