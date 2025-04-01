package transform

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.RowEncoder
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Add, Divide, EqualTo, Literal, Multiply, Not, ScalarSubquery, Subtract}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object UDFInducer {

  def main(args: Array[String]): Unit = {
    val master = if(args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("TPC-DS Q24a")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    // Define UDF for multiplying by 1.2
    val multiplyByFactor: UserDefinedFunction = udf((avgValue: Double) => avgValue * 1.2)
    spark.udf.register("multiplyByFactor", multiplyByFactor)
    val compareYear: UserDefinedFunction = udf((year: Int, comp: Int) => year == comp)
    spark.udf.register("compareYear", compareYear)

//    val q = """
//              | WITH customer_total_return AS
//              |   (SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk,
//              |           sum(sr_return_amt) AS ctr_total_return
//              |    FROM tpcds.store_returns, tpcds.date_dim
//              |    WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
//              |    GROUP BY sr_customer_sk, sr_store_sk)
//              | SELECT c_customer_id
//              |   FROM customer_total_return ctr1, tpcds.store, tpcds.customer
//              |   WHERE ctr1.ctr_total_return >
//              |    (SELECT avg(ctr_total_return)*1.2
//              |      FROM customer_total_return ctr2
//              |       WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
//              |   AND s_store_sk = ctr1.ctr_store_sk
//              |   AND s_state = 'TN'
//              |   AND ctr1.ctr_customer_sk = c_customer_sk
//              |   ORDER BY c_customer_id LIMIT 100
//              |""".stripMargin

    val q =
      """
        |SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk,
        |           sum(sr_return_amt) AS ctr_total_return
        |    FROM tpcds.store_returns, tpcds.date_dim
        |    WHERE sr_returned_date_sk = d_date_sk AND compareYear(d_year, 2000)
        |    GROUP BY sr_customer_sk, sr_store_sk
        |""".stripMargin
    val df = spark.sql(q)
    val lp = df.queryExecution.analyzed

    val newLp: LogicalPlan = lp.transformAllExpressionsWithSubqueries {
      case m @ Multiply(a, b, c) =>
        val add = Add(a, b, c)
        println(s"Transformed: $m => $add")
        add
      case eq @ EqualTo(a, b: Literal) =>
        val neq = Not(EqualTo(a, b))
        println(s"Transformed: $eq => $neq")
        neq
      case e =>
        println(s"----------Found ${e.getClass}: $e")
        e
    }

    val df2 = spark.sessionState.executePlan(newLp).executedPlan.execute()
    val df3 = spark.sessionState.executePlan(lp).executedPlan.execute()

    df2.take(5).foreach(println)
    println("==========")
    df3.take(5).foreach(println)

    println("==========")
    println(df2.toDebugString)
    println("==========")
    println(df3.toDebugString)
    println("==========")
    println(lp)
    println("==========")
    println(spark.sessionState.planner.plan(lp).next())

//    println(newLp)


//    def traversePlan(plan: LogicalPlan): Unit = {
//
//      plan.expressions.foreach { expr =>
//        expr.foreach {
//          case subquery: ScalarSubquery =>
//            traversePlan(subquery.plan)
//          case e: Add =>
//            println(s"Found => ${e.getClass}: $e")
//          case e: Subtract =>
//            println(s"Found => ${e.getClass}: $e")
//          case e: Multiply =>
//            println(s"Found => ${e.getClass}: $e")
//          case e: Divide =>
//            println(s"Found => ${e.getClass}: $e")
//          case e => // Ignore other expressions
//            println(s"${e.getClass} ... $e")
//        }
//      }
//
//      // Recursively visit children
//      plan.children.foreach(traversePlan)
//    }
//
//    // Start traversal
//    traversePlan(lp)

  }

}

/*
WithCTE
:- CTERelationDef 0, false
:  +- SubqueryAlias customer_total_return
:     +- Aggregate [sr_customer_sk#6, sr_store_sk#10], [sr_customer_sk#6 AS ctr_customer_sk#1, sr_store_sk#10 AS ctr_store_sk#2, sum(sr_return_amt#14) AS ctr_total_return#3]
:        +- Filter ((sr_returned_date_sk#23 = d_date_sk#24) AND (d_year#30 = 2000))
:           +- Join Inner
:              :- SubqueryAlias spark_catalog.tpcds.store_returns
:              :  +- Relation spark_catalog.tpcds.store_returns[sr_return_time_sk#4,sr_item_sk#5,sr_customer_sk#6,sr_cdemo_sk#7,sr_hdemo_sk#8,sr_addr_sk#9,sr_store_sk#10,sr_reason_sk#11,sr_ticket_number#12L,sr_return_quantity#13,sr_return_amt#14,sr_return_tax#15,sr_return_amt_inc_tax#16,sr_fee#17,sr_return_ship_cost#18,sr_refunded_cash#19,sr_reversed_charge#20,sr_store_credit#21,sr_net_loss#22,sr_returned_date_sk#23] parquet
:              +- SubqueryAlias spark_catalog.tpcds.date_dim
:                 +- Relation spark_catalog.tpcds.date_dim[d_date_sk#24,d_date_id#25,d_date#26,d_month_seq#27,d_week_seq#28,d_quarter_seq#29,d_year#30,d_dow#31,d_moy#32,d_dom#33,d_qoy#34,d_fy_year#35,d_fy_quarter_seq#36,d_fy_week_seq#37,d_day_name#38,d_quarter_name#39,d_holiday#40,d_weekend#41,d_following_holiday#42,d_first_dom#43,d_last_dom#44,d_same_day_ly#45,d_same_day_lq#46,d_current_day#47,... 4 more fields] parquet
+- GlobalLimit 100
   +- LocalLimit 100
      +- Sort [c_customer_id#82 ASC NULLS FIRST], true
         +- Project [c_customer_id#82]
            +- Filter (((cast(ctr_total_return#3 as decimal(24,7)) > scalar-subquery#0 [ctr_store_sk#2]) AND (s_store_sk#52 = ctr_store_sk#2)) AND ((s_state#76 = TN) AND (ctr_customer_sk#1 = c_customer_sk#81)))
               :  +- Aggregate [(avg(ctr_total_return#104) * 1.2) AS (avg(ctr_total_return) * 1.2)#106]
               :     +- Filter (outer(ctr_store_sk#2) = ctr_store_sk#103)
               :        +- SubqueryAlias ctr2
               :           +- SubqueryAlias customer_total_return
               :              +- CTERelationRef 0, true, [ctr_customer_sk#102, ctr_store_sk#103, ctr_total_return#104], false
               +- Join Inner
                  :- Join Inner
                  :  :- SubqueryAlias ctr1
                  :  :  +- SubqueryAlias customer_total_return
                  :  :     +- CTERelationRef 0, true, [ctr_customer_sk#1, ctr_store_sk#2, ctr_total_return#3], false
                  :  +- SubqueryAlias spark_catalog.tpcds.store
                  :     +- Relation spark_catalog.tpcds.store[s_store_sk#52,s_store_id#53,s_rec_start_date#54,s_rec_end_date#55,s_closed_date_sk#56,s_store_name#57,s_number_employees#58,s_floor_space#59,s_hours#60,s_manager#61,s_market_id#62,s_geography_class#63,s_market_desc#64,s_market_manager#65,s_division_id#66,s_division_name#67,s_company_id#68,s_company_name#69,s_street_number#70,s_street_name#71,s_street_type#72,s_suite_number#73,s_city#74,s_county#75,... 5 more fields] parquet
                  +- SubqueryAlias spark_catalog.tpcds.customer
                     +- Relation spark_catalog.tpcds.customer[c_customer_sk#81,c_customer_id#82,c_current_cdemo_sk#83,c_current_hdemo_sk#84,c_current_addr_sk#85,c_first_shipto_date_sk#86,c_first_sales_date_sk#87,c_salutation#88,c_first_name#89,c_last_name#90,c_preferred_cust_flag#91,c_birth_day#92,c_birth_month#93,c_birth_year#94,c_birth_country#95,c_login#96,c_email_address#97,c_last_review_date#98] parquet
 */