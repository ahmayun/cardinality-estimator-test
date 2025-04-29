package udfissues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule.coverage
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import sqlsmith.FuzzTests.withoutOptimized

import scala.collection.mutable

object ExternalAuditor {

  // Simulated external system: mapping from transaction IDs to risk scores
  private val riskScores: mutable.Map[Long, Double] = mutable.Map(
    1001L -> 0.65,
    1002L -> 0.85,
    1003L -> 0.25
  )

  // In-memory audit log for demonstration
  private val auditLog: mutable.ListBuffer[String] = mutable.ListBuffer.empty

  // Simulate fetching risk score from an external system
  def getRiskScore(txID: Long): Double = {
    // Simulate network delay
    riskScores.getOrElse(txID, 0.5)  // Default risk score if not found
  }

  // Simulate logging audit entry for a transaction
  def logAudit(txID: Long): Unit = {
    val message = s"[Audit Log] Fetched risk score for transaction $txID"
    println(message) // Simulate sending to external monitoring
    auditLog += message
  }

  // Helper to retrieve the audit log (e.g., for validation)
  def getAuditLog(): Seq[String] = auditLog.toSeq
}


object FraudDetection {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val sparkOpt = spark.sessionState.optimizer
    val excludableRules = sparkOpt.defaultBatches.flatMap(_.rules.map(_.ruleName)).toSet -- sparkOpt.nonExcludableRules.toSet
    val excludedRules = excludableRules.mkString(",")
    SQLConf.get.setConfString(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, excludedRules)


    val fetchRiskScore = udf((txID: Long) => {
      val score = ExternalAuditor.getRiskScore(txID)
      ExternalAuditor.logAudit(txID)
      score
    }).asNondeterministic()

    val schema = new StructType().add("id", "long").add("timestamp", "timestamp")

    val transactions = spark.readStream
      .format("parquet")
      .schema(schema)
      .load("/tmp/transactions")

    // Add risk score column
    val withRisk = transactions.withColumn("riskScore", fetchRiskScore($"id"))

    // Group into windows of 1 minute
    val windowed = withRisk
      .withWatermark("timestamp", "1 minute")
      .groupBy(window($"timestamp", "1 minute"))
      .agg(avg($"riskScore").alias("avgRisk"))

    // Now we can sort because aggregation has happened
    val ordered = windowed.orderBy(desc("avgRisk"))

    ordered.writeStream
      .outputMode("complete")  // required because we aggregate
      .format("console")
      .start()
      .awaitTermination()
  }
}
