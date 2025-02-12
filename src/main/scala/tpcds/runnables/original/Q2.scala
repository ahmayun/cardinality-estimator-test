package tpcds.runnables.original

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.RuleExecutor

object Q2 {


  def main(args: Array[String]): Unit = {
    val master = if(args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("TPC-DS Q2")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val q = """
              |WITH wscs as
              | (SELECT sold_date_sk, sales_price
              |  FROM (SELECT ws_sold_date_sk sold_date_sk, ws_ext_sales_price sales_price
              |        FROM tpcds.web_sales
              |        UNION ALL
              |       SELECT cs_sold_date_sk sold_date_sk, cs_ext_sales_price sales_price
              |        FROM tpcds.catalog_sales) x),
              | wswscs AS
              | (SELECT d_week_seq,
              |        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
              |        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
              |        sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
              |        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
              |        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
              |        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
              |        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
              | FROM wscs, tpcds.date_dim
              | WHERE d_date_sk = sold_date_sk
              | GROUP BY d_week_seq)
              | SELECT d_week_seq1
              |       ,round(sun_sales1/sun_sales2,2)
              |       ,round(mon_sales1/mon_sales2,2)
              |       ,round(tue_sales1/tue_sales2,2)
              |       ,round(wed_sales1/wed_sales2,2)
              |       ,round(thu_sales1/thu_sales2,2)
              |       ,round(fri_sales1/fri_sales2,2)
              |       ,round(sat_sales1/sat_sales2,2)
              | FROM
              | (SELECT wswscs.d_week_seq d_week_seq1
              |        ,sun_sales sun_sales1
              |        ,mon_sales mon_sales1
              |        ,tue_sales tue_sales1
              |        ,wed_sales wed_sales1
              |        ,thu_sales thu_sales1
              |        ,fri_sales fri_sales1
              |        ,sat_sales sat_sales1
              |  FROM wswscs,tpcds.date_dim
              |  WHERE tpcds.date_dim.d_week_seq = wswscs.d_week_seq AND d_year = 2001) y,
              | (SELECT wswscs.d_week_seq d_week_seq2
              |        ,sun_sales sun_sales2
              |        ,mon_sales mon_sales2
              |        ,tue_sales tue_sales2
              |        ,wed_sales wed_sales2
              |        ,thu_sales thu_sales2
              |        ,fri_sales fri_sales2
              |        ,sat_sales sat_sales2
              |  FROM wswscs, tpcds.date_dim
              |  WHERE tpcds.date_dim.d_week_seq = wswscs.d_week_seq AND d_year = 2001 + 1) z
              | WHERE d_week_seq1=d_week_seq2-53
              | ORDER BY d_week_seq1
              |""".stripMargin

    spark.sql(q).explain(true)

    val st = System.nanoTime()
    spark.sql(q).show(5)
    val et = System.nanoTime()

    println(s"Total time (s): ${(et-st) / 1e9}")
    println(RuleExecutor.dumpTimeSpent().split("\n").filter(!_.contains("0 / ")).mkString("\n"))
  }

}
