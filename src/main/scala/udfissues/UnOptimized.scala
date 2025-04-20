package udfissues
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import fuzzer.global.State.sparkOption
import sqlsmith.FuzzTests.withoutOptimized
import fuzzer.templates.ComplexObject
import fuzzer.exceptions._


object UnOptimized {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Fuzzer")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    val preloadedUDF = udf((s: Any) => {
      println(s"called with $s")
      val r = scala.util.Random.nextInt()
      r //ComplexObject(r,r)
    }).asNondeterministic()


    val sparkOpt = spark.sessionState.optimizer
    val excludableRules = {
      val defaultRules = sparkOpt.defaultBatches.flatMap(_.rules.map(_.ruleName)).toSet
      val rules = defaultRules -- sparkOpt.nonExcludableRules.toSet
      rules
    }
    val excludedRules = excludableRules.mkString(",")
    withoutOptimized(excludedRules) {
      val auto5 = spark.read.parquet("tpcds-min/ship_mode").as("ship_mode")
      val auto0 = spark.read.parquet("tpcds-min/item").as("item")
      val auto6 = auto5.orderBy(col("ship_mode.sm_ship_mode_id"),preloadedUDF(col("ship_mode.sm_ship_mode_sk")))
      val auto8 = auto6.select(col("ship_mode.sm_ship_mode_sk"),col("ship_mode.sm_carrier"))
      val auto7 = auto6.limit(51)
      val auto1 = auto8.join(auto0, col("ship_mode.sm_carrier") === col("item.i_brand"), "inner")
      val auto3 = auto7.join(auto0, col("ship_mode.sm_ship_mode_sk") === col("item.i_manufact_id"), "inner")
      val auto4 = auto3.select(col("item.i_brand"),col("item.i_units"))
      val sink = auto4.join(auto1, col("ship_mode.sm_ship_mode_sk") === col("item.i_class_id"), "right")
      sink.collect()
    }
  }
}


