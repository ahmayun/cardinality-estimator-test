package udfissues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import sqlsmith.FuzzTests.withoutOptimized


object UnOptimizedMin {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Fuzzer")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    val preloadedUDF = udf((s: Any) => {
      val r = scala.util.Random.nextInt(100)
      println(s"called with $s using value $r")
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
      val auto6 = auto5.orderBy(preloadedUDF(col("ship_mode.sm_ship_mode_sk")))
      auto6.show()
    }
  }
}


