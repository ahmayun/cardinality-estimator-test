package udfissues

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.{udf, col}
import scala.collection.mutable
import org.apache.spark.sql.internal.SQLConf

// Simulates a live risk score tracking system (e.g., from a fraud model)
object RiskTracker {
  private val riskScores: mutable.Map[Long, Double] = mutable.Map(
    2L -> 0.95, // very likely to be safe
    3L -> 0.5,  // uncertain
    4L -> 0.1   // suspicious
  )

  def getRisk(transactionId: Long): Double = riskScores(transactionId)

  def updateRisk(transactionId: Long, newScore: Double): Unit = {
    riskScores(transactionId) = newScore
  }
}

object FraudSuspicionRanking {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Fraud Detection: Suspicion Re-ranking")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Define a UDF that applies a non-linear transformation to stretch suspiciousness
    val suspicionScore = udf((txnId: Long) => {
      val risk = RiskTracker.getRisk(txnId)
      val suspicion = -math.log(risk) // higher when risk is low
      RiskTracker.updateRisk(txnId, suspicion) // simulate updating backend with new score
      println(f"Transaction $txnId%2d → risk = $risk%.2f → suspicion = $suspicion%.2f")
      suspicion
    }).asNondeterministic()

//      val sparkOpt = spark.sessionState.optimizer
//      val excludableRules = sparkOpt.defaultBatches.flatMap(_.rules.map(_.ruleName)).toSet -- sparkOpt.nonExcludableRules.toSet
//      val excludedRules = excludableRules.mkString(",")
//      SQLConf.get.setConfString(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, excludedRules)

    // Simulated transaction data
    spark.range(2, 5)
      .repartition(1)
      .write
      .mode("overwrite")
      .parquet("/tmp/transactions")

    // Read transactions and rank them for manual review
    val txns = spark.read.parquet("/tmp/transactions")
    val prioritized = txns.orderBy(suspicionScore(col("id")))

    println("=== Transactions ranked by updated suspicion score ===")
    prioritized.explain(true)
  }
}

/*
----------- OPTIMIZATIONS OFF-------------------
== Optimized Logical Plan ==
Project [id#7L]
+- Sort [_nondeterministic#10 ASC NULLS FIRST], true
   +- Project [id#7L, if (isnull(id#7L)) null else UDF(knownnotnull(id#7L)) AS _nondeterministic#10]
      +- Relation [id#7L] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [id#7L]
   +- Sort [_nondeterministic#10 ASC NULLS FIRST], true, 0
      +- Exchange rangepartitioning(_nondeterministic#10 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=38]
         +- Project [id#7L, if (isnull(id#7L)) null else UDF(knownnotnull(id#7L)) AS _nondeterministic#10]
            +- FileScan parquet [id#7L] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/transactions], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id:bigint>

----------- OPTIMIZATIONS ON-------------------
== Optimized Logical Plan ==
Project [id#7L]
+- Sort [_nondeterministic#10 ASC NULLS FIRST], true
   +- Project [id#7L, if (isnull(id#7L)) null else UDF(knownnotnull(id#7L)) AS _nondeterministic#10]
      +- Relation [id#7L] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [id#7L]
   +- Sort [_nondeterministic#10 ASC NULLS FIRST], true, 0
      +- Exchange rangepartitioning(_nondeterministic#10 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=47]
         +- Project [id#7L, if (isnull(id#7L)) null else UDF(knownnotnull(id#7L)) AS _nondeterministic#10]
            +- FileScan parquet [id#7L] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/transactions], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id:bigint>

 */