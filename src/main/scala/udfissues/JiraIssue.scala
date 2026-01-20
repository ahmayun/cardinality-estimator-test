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

/*
with optimizations off
Optimizer.batches.then4
Optimizer.batches.else4
Optimizer.batches.then2
Optimizer.batches.else3
FinishAnalysis.apply.case1
EliminateLimits.apply.case1
Optimizer.batches.else2
Optimizer.batches.else6
Optimizer.batches.elseif5

// with optimizations on
ColumnPruning.apply.case20
RemoveRedundantAliases.removeRedundantAliases.match6
UpdateCTERelationStats.apply.else2
RemoveRedundantAliases.removeRedundantAlias.match2
ColumnPruning.apply.else40
CollapseProject.apply.case1
EliminateSorts.recursiveRemoveSort.then1
CheckCartesianProducts.apply.then1
FinishAnalysis.apply.case1
RemoveRedundantAliases.createAttributeMapping.case1
RemoveRedundantAliases.removeRedundantAliases.match14
InferFiltersFromConstraints.apply.then1
RemoveRedundantAliases.removeRedundantAliases.case18
Optimizer.batches.then3
RemoveRedundantAliases.removeRedundantAliases.else2
RemoveRedundantAliases.removeRedundantAliases.match17
ColumnPruning.apply.case18
RemoveRedundantAliases.removeRedundantAliases.then1
EliminateSorts.apply.case3

// with optimizations off

// with optimizations on
AdaptiveSparkPlan isFinalPlan=false
+- Project [id#7L]
   +- Sort [_nondeterministic#10 ASC NULLS FIRST], true, 0
      +- Exchange rangepartitioning(_nondeterministic#10 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=63]
         +- Project [id#7L, UDF(cast(id#7L as string)) AS _nondeterministic#10]
            +- FileScan parquet [id#7L] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/jira-issue], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id:bigint>
 */

