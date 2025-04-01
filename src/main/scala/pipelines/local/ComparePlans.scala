package pipelines.local

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule.{coverage, sentinelHits}
import org.apache.spark.sql.execution.debug._
import transform.Transformer._

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.io.Source

object ComparePlans {

  class MutationSummary {
    private val m: mutable.Map[String, mutable.ListBuffer[String]] = mutable.Map()

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
        case None => m.update(queryID, mutable.ListBuffer(s))
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

  private def runQuery(spark: SparkSession, processedQuery: String): LogicalPlan = {
    val optPlan = spark.sql(processedQuery).queryExecution.optimizedPlan
    println(optPlan)
    optPlan
  }

  private def runQuery(spark: SparkSession, lp: LogicalPlan): LogicalPlan = {
    println(lp)
    lp
  }

  private def runQueryUnOpt(spark: SparkSession, processedQuery: String): LogicalPlan = {
    val analyzedPlan = spark.sql(processedQuery).queryExecution.analyzed
    println(analyzedPlan)
    analyzedPlan
  }

  def countTotalNodes(plan: LogicalPlan, nodes: mutable.Map[String, mutable.Map[Int, Int]]): Int = {
    var count = 1

    nodes.update(plan.nodeName, {
      val inner = nodes.getOrElseUpdate(plan.nodeName, mutable.Map(plan.hashCode() -> 0))
      inner.update(plan.hashCode(), inner.getOrElseUpdate(plan.hashCode(), 0) + 1)
      inner
    })
//    println(s"${plan.nodeName} (${plan.hashCode()}): ${plan.expressions.map(_.toString()).mkString(",")}")
    count += plan.children.map(n => countTotalNodes(n, nodes)).sum
    val subqueryNodes = plan.collect {
      case p if p.subqueries.nonEmpty => p.subqueries.map(n => countTotalNodes(n, nodes)).sum
    }.sum
    count + subqueryNodes
  }

  def generateScalaFiles(spark: SparkSession): Unit = {
    val queryDir = new File("src/main/scala/tpcds/queries/")
    val summary = new MutationSummary()
    val csv = mutable.ListBuffer[String]("QueryID, " +
      "Orig. Exec. Result," +
      "UDF Exec. Result," +
      "# UDFs Inserted," +
      "# Total Nodes Orig. UnOpt. Plan," +
      "# Total Nodes Orig. Opt. Plan," +
      "# Total Nodes UDF Opt Plan," +
      "# Unique Nodes Orig. UnOpt. Plan," +
      "# Unique Nodes Orig. Opt. Plan," +
      "# Unique Nodes UDF Opt Plan," +
      "Total Branch Cov. Orig.," +
      "Total Branch Cov. UDF," +
      "Unique Branch Cov. Orig," +
      "Unique Branch Cov. UDF," +
      "Orig Cov - UDF Cov," +
      "UDF Cov - Orig Cov," +
      "Sentinel Hits Orig," +
      "Sentinel Hits UDF," +
      "Sentinel Hits Union," +
      "Coverage Union"
    )

    val queryPattern = "([0-9]+)([a-z]?)".r

    queryDir
      .listFiles()
      .filter(_.getName.matches("tpcds-q\\d+[a-z]*\\.sql"))
//      .filter { file =>
//        val queryNumber = queryPattern.findFirstIn(file.getName).getOrElse("")
//        queryNumber == "93"
//      }
      .sorted
      .foreach { file =>
        val queryNumber = queryPattern.findFirstIn(file.getName).getOrElse("")
        val queryContent = Source.fromFile(file).mkString
        val processedQuery = fixTableReferences(queryContent)
        val csvLine: mutable.ListBuffer[String] = mutable.ListBuffer(s"Q$queryNumber")
        var origTotalNodeCount: Int = 0
        var origTotalNodeCountUnOpt: Int = 0
        var coverageTotalOrig = 0
        var coverageTotalOrigUnOpt = 0
        var coverageUniqueOrig = 0
        var coverageUniqueOrigUnOpt = 0
        var sentinelHitsOrig = 0
        var sentinelHitsOrigUnOpt = 0
        var coverageSetOrig: Set[String] = Set()
        var coverageSetOrigUnOpt: Set[String] = Set()
        var sentinelHitsSetOrig: Set[String] = Set()
        var sentinelHitsSetOrigUnOpt: Set[String] = Set()
        val mapOpCountsOrig = mutable.Map[String, mutable.Map[Int, Int]]()
        val mapOpCountsOrigUnOpt = mutable.Map[String, mutable.Map[Int, Int]]()
        val mapOpCountsUDF = mutable.Map[String, mutable.Map[Int, Int]]()
        val dir = new File(s"src/main/scala/plan/diff/q$queryNumber")
        if (!dir.exists())
          dir.mkdirs()

        try {
          println(s"-------------- Running Original Query $queryNumber (UnOpt) ----------------")
          val unOptPlan = runQueryUnOpt(spark, processedQuery)
          origTotalNodeCountUnOpt = countTotalNodes(unOptPlan, mapOpCountsOrigUnOpt)
          Files.write(Paths.get(s"${dir.getPath}/plan-unOpt-noUdf.txt"), unOptPlan.toString().getBytes)
          coverageTotalOrigUnOpt = coverage.length
          coverageSetOrigUnOpt = coverage.toSet
          sentinelHitsSetOrigUnOpt = sentinelHits.toSet
          coverageUniqueOrigUnOpt = coverageSetOrigUnOpt.size
          sentinelHitsOrigUnOpt = sentinelHitsSetOrigUnOpt.size
        } catch {
          case e: Throwable =>
            println(s"FAILED $e: ${e.getMessage}")
        } finally {
          coverage.clear()
          sentinelHits.clear()
        }

        try {
          println(s"-------------- Running Original Query $queryNumber (Opt) ----------------")
          val optPlan = runQuery(spark, processedQuery)
          csvLine += "PASS"
          origTotalNodeCount = countTotalNodes(optPlan, mapOpCountsOrig)
          Files.write(Paths.get(s"${dir.getPath}/plan-opt-noUdf.txt"), optPlan.toString().getBytes)
          coverageTotalOrig = coverage.length
          coverageSetOrig = coverage.toSet
          sentinelHitsSetOrig = sentinelHits.toSet
          coverageUniqueOrig = coverageSetOrig.size
          sentinelHitsOrig = sentinelHitsSetOrig.size
        } catch {
          case e: Throwable =>
            csvLine += s""""Exception: ${e.getClass}""""
            println(s"FAILED $e: ${e.getMessage}")
        } finally {
          coverage.clear()
          sentinelHits.clear()
        }

        try {
          println(s"-------------- Running UDF Query $queryNumber --------------------")
          val (plan, mutationSummary) = insertAndRegisterUDFs(spark, queryNumber, processedQuery)

          summary.merge(mutationSummary)
          println(mutationSummary)
          Files.write(Paths.get(s"${dir.getPath}/mutation-summary.txt"), mutationSummary.toString.getBytes())

          val lp = runQuery(spark, plan)

          Files.write(Paths.get(s"${dir.getPath}/plan-withUdf.txt"), lp.toString().getBytes)

          val coverageSetUDF: Set[String] = coverage.toSet
          val sentinelHitsSetUDF: Set[String] = sentinelHits.toSet
          val totalNodesUDF = countTotalNodes(plan, mapOpCountsUDF)

          Files.write(Paths.get(s"${dir.getPath}/coverage-orig.txt"), coverageSetOrig.mkString("\n").getBytes())
          Files.write(Paths.get(s"${dir.getPath}/coverage-udf.txt"), coverageSetUDF.mkString("\n").getBytes())

          val udfMinusOrig = coverageSetUDF diff coverageSetOrig
          val origMinusUDf = coverageSetOrig diff coverageSetUDF
          val coverageUnionSize = (coverageSetUDF union coverageSetOrig).size

          val udfMinusOrigSentinel = sentinelHitsSetUDF diff sentinelHitsSetOrig
          val origMinusUDFSentinel = sentinelHitsSetOrig diff sentinelHitsSetUDF
          val sentinelHitUnionSize = (sentinelHitsSetUDF union sentinelHitsSetOrig).size

          Files.write(Paths.get(s"${dir.getPath}/coveragediff_udf-orig.txt"), udfMinusOrig.mkString("\n").getBytes())
          Files.write(Paths.get(s"${dir.getPath}/coveragediff_orig-udf.txt"), origMinusUDf.mkString("\n").getBytes())
          Files.write(Paths.get(s"${dir.getPath}/sentineldiff_udf-orig.txt"), udfMinusOrigSentinel.mkString("\n").getBytes())
          Files.write(Paths.get(s"${dir.getPath}/sentineldiff_orig-udf.txt"), origMinusUDFSentinel.mkString("\n").getBytes())

          csvLine ++= mutable.ListBuffer(
            "PASS",
            mutationSummary.getInsertedCount.toString,
            origTotalNodeCountUnOpt.toString,
            origTotalNodeCount.toString,
            totalNodesUDF.toString,
            mapOpCountsOrigUnOpt.foldLeft(0){case (acc, (_, hashes)) => acc+hashes.size}.toString,
            mapOpCountsOrig.foldLeft(0){case (acc, (_, hashes)) => acc+hashes.size}.toString,
            mapOpCountsUDF.foldLeft(0){case (acc, (_, hashes)) => acc+hashes.size}.toString,
            coverageTotalOrig.toString,
            coverage.length.toString,
            coverageUniqueOrig.toString,
            coverageSetUDF.size.toString,
            origMinusUDf.size.toString,
            udfMinusOrig.size.toString,
            sentinelHitsOrig.toString,
            sentinelHits.toSet.size.toString,
            sentinelHitUnionSize.toString,
            coverageUnionSize.toString
          )

        } catch {
          case e: Throwable =>
            csvLine += s""""Exception: ${e.getClass}""""
            println(s"FAILED $e: ${e.getMessage}")
        } finally {
          coverage.clear()
          sentinelHits.clear()
        }

        println("Orig Op Counts")
        var total = 0
        mapOpCountsOrig.toSeq.sortBy(_._1).foreach {
          case (k, v) =>
            total += v.size
            println(s"$k: $v")
        }
        println(s"Total $total")
        println("--------------")
        println("UDF Op Counts")
        total = 0
        mapOpCountsUDF.toSeq.sortBy(_._1).foreach {
          case (k, v) =>
            total += v.size
            println(s"$k: $v")
        }
        println(s"Total $total")

        mapOpCountsUDF.clear()
        mapOpCountsOrig.clear()

        println(s"=================================================================")

        csv += csvLine.mkString(",")
        Files.write(Paths.get(s"${dir.getPath}/csvline.txt"), csvLine.mkString(",").getBytes)

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
