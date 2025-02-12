package tpcds.runnables.udf

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

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

    // Define UDF for multiplying by 1.2
    val multiplyByFactor: UserDefinedFunction = udf((avgValue: Double) => avgValue * 1.2)
    spark.udf.register("multiplyByFactor", multiplyByFactor)

    val q = """
              | WITH customer_total_return AS
              |   (SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk,
              |           sum(sr_return_amt) AS ctr_total_return
              |    FROM tpcds.store_returns, tpcds.date_dim
              |    WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
              |    GROUP BY sr_customer_sk, sr_store_sk)
              | SELECT c_customer_id
              |   FROM customer_total_return ctr1, tpcds.store, tpcds.customer
              |   WHERE ctr1.ctr_total_return >
              |    (SELECT multiplyByFactor(avg(ctr_total_return))
              |      FROM customer_total_return ctr2
              |       WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
              |   AND s_store_sk = ctr1.ctr_store_sk
              |   AND s_state = 'TN'
              |   AND ctr1.ctr_customer_sk = c_customer_sk
              |   ORDER BY c_customer_id LIMIT 100
              |""".stripMargin

    spark.sql(q).explain(true)

    val st = System.nanoTime()
    spark.sql(q).show(5)
    val et = System.nanoTime()

    println(s"Total time (s): ${(et-st) / 1e9}")
  }

}