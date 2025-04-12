package udfissues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.catalyst.optimizer.PushDownPredicates
import sqlsmith.FuzzTests.withoutOptimized

object OptimizerCorrectnessIssue2 {
  def main(args: Array[String]): Unit = {
    val master = if(args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("Optimizer Correctness Issue")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val ndUDF = udf((s: Int) => {
      val r = scala.util.Random.nextInt(100)
      //both values should be the same
      CustomStruct(r,r)
    }).asNondeterministic()


    val sparkOpt = spark.sessionState.optimizer
    val excludableRules = {
      val defaultRules = sparkOpt.defaultBatches.flatMap(_.rules.map(_.ruleName)).toSet
      val rules = defaultRules -- sparkOpt.nonExcludableRules.toSet
      rules.foreach(println)
      rules
    }
    val excludedRules = excludableRules.mkString(",")
    withoutOptimized(excludedRules) {
      val df2 = spark.range(5).select(ndUDF($"id").withField("c", lit(7)))
      df2.show()
      df2.collect().foreach {
        row =>
          assert(row.getStruct(0).getInt(0) == row.getStruct(0).getInt(1))
      }
    }
  }

}
