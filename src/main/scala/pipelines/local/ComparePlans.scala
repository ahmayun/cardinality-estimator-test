package pipelines.local

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Add, Divide, EqualTo, Literal, Multiply, Not, ScalaUDF, Subtract}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.DataType

import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.io.Source

object ComparePlans {

  private def fixTableReferences(query: String): String = {
    val tableNames = List("call_center","catalog_page","catalog_returns","catalog_sales","customer","customer_address","customer_demographics","date_dim","household_demographics","income_band","inventory","item","promotion","reason","ship_mode","store","store_returns","store_sales","time_dim","warehouse","web_page","web_returns","web_sales","web_site")

    tableNames.foldLeft(query) { (currentQuery, tableName) =>
      val pattern = s"\\b$tableName\\b".r
      pattern.replaceAllIn(currentQuery, s"tpcds.$tableName")
    }
  }

  def processQuery(query: String): String = {
    fixTableReferences(query)
  }

  private class MutationSummary {
    private val m: mutable.Map[String, mutable.MutableList[String]] = mutable.Map()

    def merge(other: MutationSummary): Unit = {
      m ++= other.m
    }

    def getInsertedCount: Int = {
      m.foldLeft(0) {
        case (acc, e) =>
          acc + e._2.length
      }
    }

    def logQueryInfo(queryID: String, s: String): Unit = {
      m.get(queryID) match {
        case Some(l) => l+=s
        case None => m.update(queryID, mutable.MutableList(s))
      }
    }
    override def toString: String = {
      m.map{
        case (k, v) =>
          s"""QUERY $k
             |\t${v.mkString("\n\t")}
             |""".stripMargin
      }.mkString("======MUTATION SUMMARY=============\n", "\n", "=======================")
    }
  }

  private def insertAndRegisterUDFs(spark: SparkSession, queryID: String, processedQuery: String): (LogicalPlan, MutationSummary) = {
    val lp = spark.sql(processedQuery).queryExecution.logical
    val summary = new MutationSummary()

    var udfID = 0
    val newLp: LogicalPlan = lp.transformAllExpressionsWithSubqueries {
      case x @ Multiply(l, r, _) =>
        val udf = ScalaUDF((a: Double, b: Double) => a*b, DataType.fromDDL("double"), Seq(l, r), udfName=Some(s"multiply$udfID"))
        val s = s"Transformed: $x => $udf"
        summary.logQueryInfo(queryID, s)
        println(s)
        udfID+=1
        udf
      case x @ Add(l, r, _) =>
        val udf = ScalaUDF((a: Double, b: Double) => a+b, DataType.fromDDL("double"), Seq(l, r), udfName=Some(s"add$udfID"))
        val s = s"Transformed: $x => $udf"
        summary.logQueryInfo(queryID, s)
        println(s)
        udfID+=1
        udf
      case x @ Subtract(l, r, _) =>
        val udf = ScalaUDF((a: Double, b: Double) => a-b, DataType.fromDDL("double"), Seq(l, r), udfName=Some(s"subtract$udfID"))
        val s = s"Transformed: $x => $udf"
        summary.logQueryInfo(queryID, s)
        println(s)
        udfID+=1
        udf
      case x @ Divide(l, r, _) =>
        val udf = ScalaUDF((a: Double, b: Double) => a/b, DataType.fromDDL("double"), Seq(l, r), udfName=Some(s"divide$udfID"))
        val s = s"Transformed: $x => $udf"
        summary.logQueryInfo(queryID, s)
        println(s)
        udfID+=1
        udf
      case e => e
    }


    val newOptPlan = spark.sessionState.executePlan(newLp).optimizedPlan
    (newOptPlan, summary)
  }

  private def runQuery(spark: SparkSession, processedQuery: String): LogicalPlan = {
    val optPlan = spark.sql(processedQuery).queryExecution.optimizedPlan
    println(optPlan)
    optPlan
  }

  private def runQuery(spark: SparkSession, lp: LogicalPlan): Unit = {
    println(lp)
  }

  def countNodes(plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Int = {
    var count = 1
    count += plan.children.map(countNodes).sum
    val subqueryNodes = plan.collect {
      case p if p.subqueries.nonEmpty => p.subqueries.map(countNodes).sum
    }.sum
    count + subqueryNodes
  }

  def generateScalaFiles(spark: SparkSession): Unit = {
    val queryDir = new File("src/main/scala/tpcds/queries/")
    val summary = new MutationSummary()
    val csv = mutable.MutableList[String]("QueryID, Orig. Exec. Result, UDF Exec. Result, # UDFs Inserted, # Nodes Orig. Opt. Plan, # Nodes UDF Opt Plan")

    queryDir
      .listFiles()
      .filter(_.getName.matches("tpcds-q\\d+[a-z]*\\.sql"))
      .sorted
      .take(1)
      .foreach { file =>
        val queryPattern = "([0-9]+)([a-z]?)".r
        val queryNumber = queryPattern.findFirstIn(file.getName).getOrElse("")
        val queryContent = Source.fromFile(file).mkString
        val processedQuery = processQuery(queryContent)
        val csvLine: mutable.MutableList[String] = mutable.MutableList(s"Q$queryNumber")
        var origNodeCount: Int = 0
        try {
          println(s"-------------- Running Original Query $queryNumber ----------------")
          val optPlan = runQuery(spark, processedQuery)
          csvLine += "PASS"
          origNodeCount = countNodes(optPlan)
        } catch {
          case e: Throwable =>
            csvLine += s""""Exception: ${e.getClass}""""
            println(s"FAILED $e: ${e.getMessage}")
        }

        try {
          println(s"-------------- Running UDF Query $queryNumber --------------------")
          val (plan, mutationSummary) = insertAndRegisterUDFs(spark, queryNumber, processedQuery)
          summary.merge(mutationSummary)
          println(mutationSummary)
          runQuery(spark, plan)
          println(s"=================================================================")
          csvLine += "PASS"
          csvLine += mutationSummary.getInsertedCount.toString
          csvLine += origNodeCount.toString
          csvLine += countNodes(plan).toString
        } catch {
          case e: Throwable =>
            csvLine += s""""Exception: ${e.getClass}""""
            println(s"FAILED $e: ${e.getMessage}")
        }
        csv += csvLine.mkString(",")
    }
    println(summary)
    println(csv.mkString("\n"))
  }

  def main(args: Array[String]): Unit = {
    val master = if(args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("Compare Plans")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    generateScalaFiles(spark)
  }
}
