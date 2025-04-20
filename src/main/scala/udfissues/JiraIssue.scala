package udfissues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import sqlsmith.FuzzTests.withoutOptimized


object DummyRedisClient {
  private var state: Int = 1
  private var increasing: Boolean = true

  def get(key: String): String = {
    val result = state.toString

    // Update state for next call
    if (increasing) {
      if (state < 3) {
        state += 1
      } else {
        state -= 1
        increasing = false
      }
    } else {
      if (state > 0) {
        state -= 1
      } else {
        state += 1
        increasing = true
      }
    }

    result
  }
}

object JiraIssue {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val myUdf = udf((input: String) => {
      val state = DummyRedisClient.get("global_state").toInt
      println(s"returning $state for $input")
      state
    }).asNondeterministic()

    // Disable all excludable optimizer rules
    val sparkOpt = spark.sessionState.optimizer
    val excludableRules = sparkOpt.defaultBatches.flatMap(_.rules.map(_.ruleName)).toSet -- sparkOpt.nonExcludableRules.toSet
    val excludedRules = excludableRules.mkString(",")
    SQLConf.get.setConfString(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, excludedRules)

    spark.range(1, 4).repartition(1).write.mode("overwrite").parquet("/tmp/jira-issue")
    val a = spark.read.parquet("/tmp/jira-issue")
    val b = a.orderBy(myUdf(col("id")))
    b.show()

  }
}


