package udfissues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable

// Simulate external state manager (e.g., job scheduler)
object StateManager {
  private val stateMap: mutable.Map[Long, Double] = mutable.Map (
    2L -> 0.1,
    3L -> 0.2,
    4L -> 0.3
  )

  def get(key: Long): Double = {
    stateMap(key)
  }

  def set(key: Long, value: Double): Unit = {
    stateMap(key) = value
  }
}

object MotivatingExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val assignSlotUdf = udf((arg: Long) => {
      val state = StateManager.get(arg)
      val newState = -math.log(state)
      StateManager.set(arg, newState)
      println(s"state[$arg] = $state => $newState")
      newState
    }).asNondeterministic()

    // Disable all excludable optimizer rules
//    val sparkOpt = spark.sessionState.optimizer
//    val excludableRules = sparkOpt.defaultBatches.flatMap(_.rules.map(_.ruleName)).toSet -- sparkOpt.nonExcludableRules.toSet
//    val excludedRules = excludableRules.mkString(",")
//    SQLConf.get.setConfString(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, excludedRules)

    // Emulate data source
    spark.range(2, 5)
      .repartition(1)
      .write
      .mode("overwrite")
      .parquet("/tmp/jira-issue")

    // Minimal query
    val a = spark.read.parquet("/tmp/jira-issue")
    val b = a.orderBy(assignSlotUdf(col("id")))
    b.show()
  }
}
