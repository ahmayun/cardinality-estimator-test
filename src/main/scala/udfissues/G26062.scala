package udfissues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import fuzzer.global.State.sparkOption
import sqlsmith.FuzzTests.withoutOptimized
import fuzzer.templates.ComplexObject
import fuzzer.exceptions._


object G26062 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("G26062_OURS")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    val preloadedUDF = udf((s: Any) => {
      val r = scala.util.Random.nextInt()
      println("called")
      ComplexObject(r,r)
    }).asNondeterministic()


    val sparkOpt = spark.sessionState.optimizer
    val excludableRules = {
      val defaultRules = sparkOpt.defaultBatches.flatMap(_.rules.map(_.ruleName)).toSet
      val rules = defaultRules -- sparkOpt.nonExcludableRules.toSet
      rules
    }
    val excludedRules = excludableRules.mkString(",")
    withoutOptimized(excludedRules) {
      val auto5 = spark.table("tpcds.web_site").as("web_site")
      val auto0 = spark.table("tpcds.item").as("item")
      val auto6 = auto5.orderBy(preloadedUDF(col("web_site.web_class")),col("web_site.web_manager"))
      val auto7 = auto6.limit(56)
      val auto3 = auto7.join(auto0, col("web_site.web_mkt_id") === col("item.i_manager_id"), "right")
      val auto4 = auto3.as("Xxksn")
      val auto1 = auto4.join(auto0, col("Xxksn.web_tax_percentage") === col("Xxksn.i_current_price"), "left")
      val auto2 = auto1.limit(4)
      val auto8 = auto5.join(auto2, col("Xxksn.web_site_id") === col("Xxksn.i_product_name"), "inner")
      val sink = auto8.select(col("Xxksn.web_rec_start_date"),col("Xxksn.i_category_id"))
      sink.explain(true)
      sink.show()

    }
  }
}
