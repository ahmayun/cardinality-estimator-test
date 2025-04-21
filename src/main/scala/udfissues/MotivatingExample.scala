package udfissues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf

// Simulate external state manager (e.g., job scheduler)
object DummyScheduler {
  private var jobCounter = 1000

  def reserveSlot(jobId: Long): Int = {
    val assignedSlot = jobCounter
    jobCounter += 1 // Advance the counter
    println(s"Reserving slot $assignedSlot for $jobId")
    assignedSlot
  }
}

object MotivatingExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // UDF that reserves a job slot
    val assignSlotUdf = udf((jobId: Long) => {
      DummyScheduler.reserveSlot(jobId)
    }).asNondeterministic()

    // Disable all excludable optimizer rules
    val sparkOpt = spark.sessionState.optimizer
    val excludableRules = sparkOpt.defaultBatches.flatMap(_.rules.map(_.ruleName)).toSet -- sparkOpt.nonExcludableRules.toSet
    val excludedRules = excludableRules.mkString(",")
    SQLConf.get.setConfString(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, excludedRules)

    // Emulate data source
    spark.range(1, 4)
      .repartition(1)
      .write
      .mode("overwrite")
      .parquet("/tmp/jira-issue")

    // Minimal query
    val a = spark.read.parquet("/tmp/jira-issue")
    val b = a.orderBy(assignSlotUdf(col("id")))
    b.show(false)
  }
}
