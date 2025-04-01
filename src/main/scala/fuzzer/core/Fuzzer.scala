package fuzzer.core

import org.apache.spark.sql.SparkSession
import pipelines.local.ComparePlans.countTotalNodes
import sqlsmith.FuzzTests.withoutOptimized
import transform.Transformer.fixTableReferences

import scala.collection.mutable

object Fuzzer {

  private case class FuzzResults() {
    override def toString: String = {
      "[DUMMY RESULTS]"
    }
  }

  private case class ExecutionResult() {

  }

  private def setupEnvironment(config: Map[String, String]): SparkSession = {
    val spark = SparkSession.builder()
      .appName("Compare Plans")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master(config("master"))
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  private def extractMaster(args: Array[String]): String = {
    args(0)
  }
  private def extractIterations(args: Array[String]): String = {
    args(1)
  }
  private def extractConfigFromArgs(args: Array[String]): Map[String, String] = {
    val default = args.isEmpty

    if (default) {
      Map[String, String] (
        "master" -> "local[*]", // master node for spark
        "iterations" -> "1", // Number of iterations to run the fuzzing for, mutually exclusive with duration option
        "duration" -> "60", // Duration of fuzzing, should be mutually exclusive with iterations
        "cbfreq" -> "100" // Frequency of the results callback (after how many iterations should results be printed/processed)
      )
    } else {
      Map[String, String] (
        "master" -> extractMaster(args),
        "iterations" -> extractIterations(args),
        "duration" -> "60",
      )
    }

  }

  private val resultsCallback: () => Unit = () => {
    println("Callback called")
  }

  private def extractStopCondition(config: Map[String, String]): () => Boolean = {
    val iterations = config("iterations").toInt
    var currentIteration = 0

    () => {
      if (currentIteration < iterations) {
        currentIteration += 1
        true
      } else {
        false
      }
    }
  }

  private def executeLogical(spark: SparkSession, query: String): ExecutionResult = {
    // This function needs to run the query with optimizations disabled
    val sparkOpt = spark.sessionState.optimizer
    val excludableRules = {
      val defaultRules = sparkOpt.defaultBatches.flatMap(_.rules.map(_.ruleName)).toSet
      val rules = defaultRules -- sparkOpt.nonExcludableRules.toSet
      rules
    }
    val nodeCountsOpt = mutable.Map[String, mutable.Map[Int, Int]]()
    val plan = spark.sql(query).queryExecution.optimizedPlan
    countTotalNodes(plan,nodeCountsOpt)
    println(s"Optimizations ON: ${nodeCountsOpt.foldLeft(0){case (acc, (_, hashes)) => acc+hashes.size}}")

    val nodeCountsUnOpt = mutable.Map[String, mutable.Map[Int, Int]]()

    val excludedRules = excludableRules.mkString(",")
    withoutOptimized(excludedRules) {
      val unOptPlan = spark.sql(query).queryExecution.optimizedPlan
      countTotalNodes(unOptPlan,nodeCountsUnOpt)
      println(s"Optimizations OFF: ${nodeCountsUnOpt.foldLeft(0){case (acc, (_, hashes)) => acc+hashes.size}}")
    }
    ExecutionResult()
  }

  private def fuzz(spark: SparkSession, config: Map[String, String], queryGen:() => String, callback: () => Unit): FuzzResults = {
    var i = 0
    val shouldContinue = extractStopCondition(config)
    while (shouldContinue()) {
      val query = queryGen()
      val res1 = executeLogical(spark, query)


      if(i % config("cbfreq").toInt == 0) {
        callback()
      }

      i+=1
    }
    FuzzResults()
  }

  private def constructQueryGenerator(spark: SparkSession, fuzzerConfig: Map[String, String]): () => String = {

    () => {
      val queryContent = """
                           | WITH customer_total_return AS
                           |   (SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk,
                           |           sum(sr_return_amt) AS ctr_total_return
                           |    FROM store_returns, date_dim
                           |    WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
                           |    GROUP BY sr_customer_sk, sr_store_sk)
                           | SELECT c_customer_id
                           |   FROM customer_total_return ctr1, store, customer
                           |   WHERE ctr1.ctr_total_return >
                           |    (SELECT avg(ctr_total_return)*1.2
                           |      FROM customer_total_return ctr2
                           |       WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
                           |   AND s_store_sk = ctr1.ctr_store_sk
                           |   AND s_state = 'TN'
                           |   AND ctr1.ctr_customer_sk = c_customer_sk
                           |   ORDER BY c_customer_id LIMIT 100
                           |""".stripMargin
      val processedQuery = fixTableReferences(queryContent)
      processedQuery
    }
  }


  def main(args: Array[String]): Unit = {
    val fuzzerConfig = extractConfigFromArgs(args)
    val spark = setupEnvironment(fuzzerConfig)
    val queryGenerator = constructQueryGenerator(spark, fuzzerConfig)
    val results = fuzz(spark, fuzzerConfig, queryGenerator, resultsCallback)
    println(results)
  }


}
