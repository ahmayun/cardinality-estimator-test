package misc

import com.databricks.spark.sql.perf.tpcds.TPCDS
import com.databricks.spark.sql.perf.mllib.MLBenchmarks.sqlContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule.ruleApplicationCounts

object TPCDSTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("QueryOptTester")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val tpcds = new TPCDS (sqlContext = sqlContext)

    val databaseName = "tpcds"
    sqlContext.sql(s"use $databaseName")
    val resultLocation = "results/tpcds_results"
    val iterations = 1 // how many iterations of queries to run.
    val queries = tpcds.tpcds2_4Queries // queries to run.
    val timeout = 24*60*60 // timeout, in seconds.

    var total = 0
    clearRuleMap
    queries.foreach{ q =>
      q.sqlText match {
        case Some(qstr) =>
          total += 1
          sqlContext.sql(qstr).explain(true)
        case None =>
          println("Query not found")
      }
    }
    println(s"# queries: $total/${queries.length}")
    println(ruleApplicationCounts.mkString("\n"))
    println(ruleApplicationCounts.size)

//    sqlContext.sql()
//    val experiment = tpcds.runExperiment(
//      queries.take(2),
//      iterations = iterations,
//      resultLocation = resultLocation,
//      forkThread = true)
//    experiment.waitForFinish(timeout)

  }

  def clearRuleMap: Unit = {
    ruleApplicationCounts.foreach {
      case (k, _) =>
        ruleApplicationCounts.remove(k)
    }
  }

}
