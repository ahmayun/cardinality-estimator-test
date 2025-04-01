package tpcds.runnables.generated.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/*
======MUTATION SUMMARY=============
QUERY 1
	Transformed: ('avg('ctr_total_return) * 1.2) => multiply0('avg('ctr_total_return), 1.2)
=======================
*/
object Q1 {

  def main(args: Array[String]): Unit = {
    val master = if(args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("TPC-DS Q1")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val q = """
	|GlobalLimit 100
	|+- LocalLimit 100
	|   +- Sort [c_customer_id#82 ASC NULLS FIRST], true
	|      +- Project [c_customer_id#82]
	|         +- Join Inner, (ctr_customer_sk#1 = c_customer_sk#81)
	|            :- Project [ctr_customer_sk#1]
	|            :  +- Join Inner, (s_store_sk#52 = ctr_store_sk#2)
	|            :     :- Project [ctr_customer_sk#1, ctr_store_sk#2]
	|            :     :  +- Filter (cast(ctr_total_return#3 as double) > if (isnull(alwaysTrue#269)) multiply0(null, 1.2) else multiply0(avg(ctr_total_return), 1.2)#166)
	|            :     :     +- Join LeftOuter, (ctr_store_sk#2 = ctr_store_sk#163)
	|            :     :        :- Filter isnotnull(ctr_total_return#3)
	|            :     :        :  +- Aggregate [sr_customer_sk#6, sr_store_sk#10], [sr_customer_sk#6 AS ctr_customer_sk#1, sr_store_sk#10 AS ctr_store_sk#2, MakeDecimal(sum(UnscaledValue(sr_return_amt#14)),17,2) AS ctr_total_return#3]
	|            :     :        :     +- Project [sr_customer_sk#6, sr_store_sk#10, sr_return_amt#14]
	|            :     :        :        +- Join Inner, (sr_returned_date_sk#23 = d_date_sk#24)
	|            :     :        :           :- Project [sr_customer_sk#6, sr_store_sk#10, sr_return_amt#14, sr_returned_date_sk#23]
	|            :     :        :           :  +- Filter (((isnotnull(sr_returned_date_sk#23) AND isnotnull(sr_store_sk#10)) AND isnotnull(sr_customer_sk#6)) AND dynamicpruning#271 [sr_returned_date_sk#23])
	|            :     :        :           :     :  +- Project [d_date_sk#24]
	|            :     :        :           :     :     +- Filter ((isnotnull(d_year#30) AND (d_year#30 = 2000)) AND isnotnull(d_date_sk#24))
	|            :     :        :           :     :        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#24,d_date_id#25,d_date#26,d_month_seq#27,d_week_seq#28,d_quarter_seq#29,d_year#30,d_dow#31,d_moy#32,d_dom#33,d_qoy#34,d_fy_year#35,d_fy_quarter_seq#36,d_fy_week_seq#37,d_day_name#38,d_quarter_name#39,d_holiday#40,d_weekend#41,d_following_holiday#42,d_first_dom#43,d_last_dom#44,d_same_day_ly#45,d_same_day_lq#46,d_current_day#47,... 4 more fields] parquet
	|            :     :        :           :     +- Relation spark_catalog.tpcds.store_returns[sr_return_time_sk#4,sr_item_sk#5,sr_customer_sk#6,sr_cdemo_sk#7,sr_hdemo_sk#8,sr_addr_sk#9,sr_store_sk#10,sr_reason_sk#11,sr_ticket_number#12L,sr_return_quantity#13,sr_return_amt#14,sr_return_tax#15,sr_return_amt_inc_tax#16,sr_fee#17,sr_return_ship_cost#18,sr_refunded_cash#19,sr_reversed_charge#20,sr_store_credit#21,sr_net_loss#22,sr_returned_date_sk#23] parquet
	|            :     :        :           +- Project [d_date_sk#24]
	|            :     :        :              +- Filter ((isnotnull(d_year#30) AND (d_year#30 = 2000)) AND isnotnull(d_date_sk#24))
	|            :     :        :                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#24,d_date_id#25,d_date#26,d_month_seq#27,d_week_seq#28,d_quarter_seq#29,d_year#30,d_dow#31,d_moy#32,d_dom#33,d_qoy#34,d_fy_year#35,d_fy_quarter_seq#36,d_fy_week_seq#37,d_day_name#38,d_quarter_name#39,d_holiday#40,d_weekend#41,d_following_holiday#42,d_first_dom#43,d_last_dom#44,d_same_day_ly#45,d_same_day_lq#46,d_current_day#47,... 4 more fields] parquet
	|            :     :        +- Aggregate [ctr_store_sk#163], [multiply0(avg(ctr_total_return#164), 1.2) AS multiply0(avg(ctr_total_return), 1.2)#166, ctr_store_sk#163, true AS alwaysTrue#269]
	|            :     :           +- Aggregate [sr_customer_sk#220, sr_store_sk#224], [sr_store_sk#224 AS ctr_store_sk#163, MakeDecimal(sum(UnscaledValue(sr_return_amt#228)),17,2) AS ctr_total_return#164]
	|            :     :              +- Project [sr_customer_sk#220, sr_store_sk#224, sr_return_amt#228]
	|            :     :                 +- Join Inner, (sr_returned_date_sk#237 = d_date_sk#238)
	|            :     :                    :- Project [sr_customer_sk#220, sr_store_sk#224, sr_return_amt#228, sr_returned_date_sk#237]
	|            :     :                    :  +- Filter ((isnotnull(sr_returned_date_sk#237) AND isnotnull(sr_store_sk#224)) AND dynamicpruning#272 [sr_returned_date_sk#237])
	|            :     :                    :     :  +- Project [d_date_sk#238]
	|            :     :                    :     :     +- Filter ((isnotnull(d_year#244) AND (d_year#244 = 2000)) AND isnotnull(d_date_sk#238))
	|            :     :                    :     :        +- Relation spark_catalog.tpcds.date_dim[d_date_sk#238,d_date_id#239,d_date#240,d_month_seq#241,d_week_seq#242,d_quarter_seq#243,d_year#244,d_dow#245,d_moy#246,d_dom#247,d_qoy#248,d_fy_year#249,d_fy_quarter_seq#250,d_fy_week_seq#251,d_day_name#252,d_quarter_name#253,d_holiday#254,d_weekend#255,d_following_holiday#256,d_first_dom#257,d_last_dom#258,d_same_day_ly#259,d_same_day_lq#260,d_current_day#261,... 4 more fields] parquet
	|            :     :                    :     +- Relation spark_catalog.tpcds.store_returns[sr_return_time_sk#218,sr_item_sk#219,sr_customer_sk#220,sr_cdemo_sk#221,sr_hdemo_sk#222,sr_addr_sk#223,sr_store_sk#224,sr_reason_sk#225,sr_ticket_number#226L,sr_return_quantity#227,sr_return_amt#228,sr_return_tax#229,sr_return_amt_inc_tax#230,sr_fee#231,sr_return_ship_cost#232,sr_refunded_cash#233,sr_reversed_charge#234,sr_store_credit#235,sr_net_loss#236,sr_returned_date_sk#237] parquet
	|            :     :                    +- Project [d_date_sk#238]
	|            :     :                       +- Filter ((isnotnull(d_year#244) AND (d_year#244 = 2000)) AND isnotnull(d_date_sk#238))
	|            :     :                          +- Relation spark_catalog.tpcds.date_dim[d_date_sk#238,d_date_id#239,d_date#240,d_month_seq#241,d_week_seq#242,d_quarter_seq#243,d_year#244,d_dow#245,d_moy#246,d_dom#247,d_qoy#248,d_fy_year#249,d_fy_quarter_seq#250,d_fy_week_seq#251,d_day_name#252,d_quarter_name#253,d_holiday#254,d_weekend#255,d_following_holiday#256,d_first_dom#257,d_last_dom#258,d_same_day_ly#259,d_same_day_lq#260,d_current_day#261,... 4 more fields] parquet
	|            :     +- Project [s_store_sk#52]
	|            :        +- Filter ((isnotnull(s_state#76) AND (s_state#76 = TN)) AND isnotnull(s_store_sk#52))
	|            :           +- Relation spark_catalog.tpcds.store[s_store_sk#52,s_store_id#53,s_rec_start_date#54,s_rec_end_date#55,s_closed_date_sk#56,s_store_name#57,s_number_employees#58,s_floor_space#59,s_hours#60,s_manager#61,s_market_id#62,s_geography_class#63,s_market_desc#64,s_market_manager#65,s_division_id#66,s_division_name#67,s_company_id#68,s_company_name#69,s_street_number#70,s_street_name#71,s_street_type#72,s_suite_number#73,s_city#74,s_county#75,... 5 more fields] parquet
	|            +- Project [c_customer_sk#81, c_customer_id#82]
	|               +- Filter isnotnull(c_customer_sk#81)
	|                  +- Relation spark_catalog.tpcds.customer[c_customer_sk#81,c_customer_id#82,c_current_cdemo_sk#83,c_current_hdemo_sk#84,c_current_addr_sk#85,c_first_shipto_date_sk#86,c_first_sales_date_sk#87,c_salutation#88,c_first_name#89,c_last_name#90,c_preferred_cust_flag#91,c_birth_day#92,c_birth_month#93,c_birth_year#94,c_birth_country#95,c_login#96,c_email_address#97,c_last_review_date#98] parquet
""".stripMargin

    spark.sql(q).explain(true)

    val st = System.nanoTime()
    spark.sql(q).show(5)
    val et = System.nanoTime()

    println(s"Total time (s): ${(et-st) / 1e9}")
    println(RuleExecutor.dumpTimeSpent().split("\n").filter(!_.contains("0 / ")).mkString("\n"))
  }
}
