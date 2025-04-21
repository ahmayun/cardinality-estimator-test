/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sqlsmith

import java.io.{File, FileWriter, PrintWriter}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, Statement}
import java.util.UUID
import scala.collection.mutable
import scala.util.control.NonFatal
import com.google.common.io.Files
import fuzzer.core.CampaignStats
import fuzzer.core.MainFuzzer.{constructCombinedFileContents, deleteDir, prettyPrintStats, writeLiveStats}
import fuzzer.exceptions.{MismatchException, Success}
import fuzzer.global.FuzzerConfig
import fuzzer.oracle.OracleSystem
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types._
import sqlsmith.loader.SQLSmithLoader
import sqlsmith.loader.Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule.coverage
import org.apache.spark.sql.functions.expr
//import java.nio.file.Files


object FuzzTests {

  private lazy val sqlSmithApi = SQLSmithLoader.loadApi()

  // Loads the SQLite JDBC driver
//  Class.forName("org.sqlite.JDBC")

  private def debugPrint(s: String): Unit = {
    // scalastyle:off println
    println(s)
    // scalastyle:on println
  }

  def makeDivider(title: String = ""): String = {
    s"============================${title}========================================"
  }

  def dumpSparkCatalog(spark: SparkSession, tables: Seq[Table]): String = {
    val dbFile = new File(Utils.createTempDir(), s"dumped-spark-tables-${UUID.randomUUID}.db")
    val conninfo = s"jdbc:sqlite:${dbFile.getAbsolutePath}"

    var conn: Connection = null
    var stmt: Statement = null
    try {
      conn = DriverManager.getConnection(conninfo)
      stmt = conn.createStatement()

      tables.foreach { t =>
        val tableIdentifier = if (t.database != null) s"${t.database}.${t.name}" else t.name
        val schema = spark.table(tableIdentifier).schema

        def toSQLiteTypeName(dataType: DataType): Option[String] = dataType match {
          case ByteType | ShortType | IntegerType | LongType | _: DecimalType => Some("INTEGER")
          case FloatType | DoubleType => Some("REAL")
          case StringType => Some("TEXT")
          case tpe =>
            debugPrint(s"Cannot handle ${tpe.catalogString} in $tableIdentifier")
            None
        }

        val attrDefs = schema.flatMap { f =>
          toSQLiteTypeName(f.dataType).map(tpe => s"${f.name} $tpe")
        }
        if (attrDefs.nonEmpty) {
          val ddlStr =
            s"""
               |CREATE TABLE ${t.name} (
               |  ${attrDefs.mkString(",\n  ")}
               |);
             """.stripMargin

          debugPrint(
            s"""
               |SQLite DDL String from a Spark schema: `${schema.toDDL}`:
               |$ddlStr
             """.stripMargin)

          try{
            stmt.execute(ddlStr)
          } catch {
            case e =>
              println(s"Exception ${e}")
          }
        } else {
          debugPrint(s"Valid schema not found: $tableIdentifier")
        }
      }
    } finally {
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
    dbFile.getAbsolutePath
  }

  private def withSQLConf[T](pairs: (String, String)*)(f: => T): T = {
    val conf = SQLConf.get
    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (conf.contains(key)) {
        Some(conf.getConfString(key))
      } else {
        None
      }
    }
    keys.lazyZip(values).foreach { (k, v) =>
//      assert(!SQLConf.staticConfKeys.contains(k))
      conf.setConfString(k, v)
    }
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => conf.setConfString(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
  }

  def writeQueryToFile(query: String, id: String, path: String): Unit = {
    val directory = new File(path)
    if (!directory.exists()) {
      directory.mkdirs() // Create the directory if it doesn't exist
    }

    val filePath = s"$path/query_$id.txt"
    val file = new File(filePath)

    val writer = new PrintWriter(file)
    try {
      writer.write(query) // Write the query to the file
    } finally {
      writer.close() // Close the writer to release resources
    }
  }
  def countSelectStatements(query: String): (String, Int) = {
    val logicalPlan = CatalystSqlParser.parsePlan(query)
    ("# selects", {
      val logicalPlan = CatalystSqlParser.parsePlan(query)

      // Traverse the LogicalPlan and count all Project nodes
      logicalPlan.collect {
        case _: Project => 1
      }.sum
    })
  }

  def maxNestedDepth(query: String): (String, Int) = {
    val logicalPlan = CatalystSqlParser.parsePlan(query)
    def getDepth(plan: LogicalPlan, depth: Int): Int = {
      plan.children.map(child => getDepth(child, depth + 1)).foldLeft(depth)(math.max)
    }
    ("Max nesting", getDepth(logicalPlan, 1))
  }

  def countReferencedTables(query: String): (String, Int) = {
    val logicalPlan = CatalystSqlParser.parsePlan(query)
    ("# referenced tables", logicalPlan.collect {
      case relation: UnresolvedRelation => relation.tableName
    }.distinct.length)
  }
  case class IntTree(value: Int, children: Seq[IntTree]) {


    def total: Int = {
      value + children.map(_.total).sum
    }

    def prettyPrint(indent: String = ""): String = {
      val currentLevel = s"${indent}Value: $value\n"
      val childrenLevels = children.map(_.prettyPrint(indent + "  ")).mkString
      currentLevel + childrenLevels
    }

    override def toString: String = prettyPrint()
  }

  def countReferencedTablesTree(plan: LogicalPlan): IntTree = {
    // Count tables at the current level
    val currentCount = plan.collect {
      case relation: UnresolvedRelation => relation.tableName
    }.distinct.length

    // Recursively compute for children
    val childrenCounts = plan.children.map(countReferencedTablesTree)
    IntTree(currentCount, childrenCounts)
  }

  def countReferencedTablesTree(query: String): (String, IntTree) = {
    val logicalPlan = CatalystSqlParser.parsePlan(query)
    ("# referenced tables", countReferencedTablesTree(logicalPlan))
  }


  def countReferencedColumns(query: String): (String, Int) = {
    val logicalPlan = CatalystSqlParser.parsePlan(query)
    ("# referenced columns", logicalPlan.collect {
      case project: Project => project.projectList.size
    }.sum)
  }

  def harvestSubqueriesFromExpression(e: Expression): Option[IntTree] = {
    e match {
      case scalar: org.apache.spark.sql.catalyst.expressions.ScalarSubquery =>
        Some(countReferencedColumnsTree(scalar.plan))
      case _ if e.children.nonEmpty =>
        Some(IntTree(0, e.children.flatMap(e => harvestSubqueriesFromExpression(e))))
      case _ =>
        None
    }
  }
  def harvestSubqueries(plan: LogicalPlan): Seq[IntTree] = {
    plan.expressions.flatMap { e =>
      harvestSubqueriesFromExpression(e)
    }
  }

  def countReferencedColumnsTree(plan: LogicalPlan): IntTree = {
    // Count columns at the current level

    // Traverse expressions to find and process scalar subqueries
    val scalarSubqueryCounts = harvestSubqueries(plan)
//    plan.expressions.foreach{ expr =>
//      println(s"Expr:\n${expr}")
//    }
    val currentCount = plan match {
      case project: Project => project.projectList.size
      case _ => 0
    }

    // Recursively compute for children of the logical plan
    val childrenCounts = plan.children.map(countReferencedColumnsTree)

    // Combine children counts and scalar subquery counts
    IntTree(currentCount, childrenCounts ++ scalarSubqueryCounts)
  }

  def countReferencedColumnsTree(query: String): (String, Int) = {
    val logicalPlan = CatalystSqlParser.parsePlan(query)
    val tree = countReferencedColumnsTree(logicalPlan)
    println(tree)
    ("# referenced columns", tree.total)
  }

  def sizeOfExpression(query: String): (String, Int) = {
    ("Query size (bytes)", query.getBytes("UTF-8").length) // UTF-8 encoding to calculate the exact byte size
  }

  def returnTypeOfExpression(spark: SparkSession, expression: String): (String, String) = {
    val df = spark.range(1).select(expr(expression))
    ("Return type", df.schema.fields.head.dataType.simpleString)
  }
  val metricComputers: Array[String=>(String, Any)] = Array[String=>(String, Any)](
    countSelectStatements,
    countReferencedColumnsTree,
    countReferencedTablesTree,
    maxNestedDepth,
    sizeOfExpression
  )

  def setupSpark(master: String, arguments: FuzzerArguments): SparkSession = {
    val spark = if(arguments.hive) {
      SparkSession.builder()
        .appName("FuzzTest")
        .master(master)
        .enableHiveSupport()
        .getOrCreate()
    } else {
      SparkSession.builder()
        .appName("FuzzTest")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .master(master)
        .getOrCreate()
    }
    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

  def setupEnv(): Unit = {
    Class.forName("org.sqlite.JDBC")
  }

  def gatherTargetTables(spark: SparkSession): Array[Table] = {
    spark
      .catalog
      .listDatabases()
      .collect()
      .flatMap(db => spark.catalog.listTables(db.name).collect())
      .filter(_.database == "main") // sqlsmith will only reference main so others not needed
  }

  def generateSqlSmithQuery(sqlSmithSchema: Long): String = {
    try{
      sqlSmithApi.getSQLFuzz(sqlSmithSchema)
//      "select count(*) from main.customer inner join main.web_sales on ws_ship_customer_sk == c_customer_sk"
    } catch {
      case e: Throwable =>
        sqlSmithApi.free(sqlSmithSchema)
        System.err.println(
          s"""
            |"Failed to generate query!"
            |$e
            |""".stripMargin
        )
        sys.exit(-1)
    }
  }

  def getEstimatedCount(spark: SparkSession, queryStr: String): BigInt = {
    spark.sql(queryStr).queryExecution.optimizedPlan.stats.rowCount match {
      case Some(c) => c
      case None => BigInt(-1)
    }
  }

  def mkStatsStr(estCount: BigInt,
                 actualCount: Long,
                 startTime: Long,
                 endTime: Long,
                 cpuTime: Long,
                 peakMem: Long): String = {

    val duration = (endTime - startTime) / 1e9
    val absDiff = (estCount-actualCount).abs

    val output =
      s"""
         |Actual Count: $actualCount
         |Estimated Count: ${if (estCount < 0) "No Estimate" else estCount}
         |Abs diff: $absDiff
         |Exec time: $duration seconds
         |CPU Time: $cpuTime
         |Peak Mem: $peakMem
         |""".stripMargin
    output
  }

  def printAndWriteStats(fail: Boolean,
                         resultsDir: String,
                         queryStr: String,
                         numStmtGenerated: Long,
                         metrics: Array[String],
                         estCount: BigInt,
                         plan: String,
                         actualCount: Long,
                         startTime: Long,
                         endTime: Long,
                         cpuTime: Long,
                         peakMem: Long): String = {

    val sign = if (estCount-actualCount >= 0) "+" else "-"
    val absDiff = (estCount-actualCount).abs

    val statsStr = mkStatsStr(estCount, actualCount, startTime, endTime, cpuTime, peakMem)
    val output =
      s"""
         |${makeDivider("STATS")}
         |$statsStr
         |${metrics.mkString("\n")}
         |${makeDivider("QUERY")}
         |$queryStr
         |${makeDivider("PLAN")}
         |""".stripMargin


    val status = if(!fail) "PASS" else "ERR"
    if(estCount < 0) {
      writeQueryToFile(output, s"${numStmtGenerated}_${actualCount}", s"$resultsDir/queries/$status/NO_ESTIMATES")
    } else if (absDiff > 0) {
      writeQueryToFile(output, s"${numStmtGenerated}_${sign}${absDiff}", s"$resultsDir/queries/$status/DIFFERENT_ESTIMATES")
    } else {
      writeQueryToFile(output, s"${numStmtGenerated}", s"$resultsDir/queries/$status/ACCURATE_ESTIMATES")
    }

    statsStr

  }


  def determinErrorType(e: Throwable): String = {
    e match {
      case _: org.apache.spark.sql.catalyst.parser.ParseException =>
        "ERR_PARSE"
      case _: org.apache.spark.sql.AnalysisException =>
        "ERR_ANLYS"
      case _ =>
        "ERR_OTHER"
    }
  }

  def compareAndWrite(fail: Boolean,
                      resultsDir: String,
                      queryStr: String,
                      numStmtGenerated: Long,
                      plans: (String, String),
                      counts: (Long, Long),
                      estCounts: (BigInt, BigInt),
                      startTimes: (Long, Long),
                      endTimes: (Long, Long),
                      cpuTimes: (Long, Long),
                      peakMems: (Long, Long)): Unit = {


    val (startTimeOpt, startTimeUnOpt) = startTimes
    val (endTimeOpt, endTimeUnOpt) = endTimes
    val (durationOpt, durationUnOpt) = ((endTimeOpt-startTimeOpt) / 1e9, (endTimeUnOpt-startTimeUnOpt) / 1e9)
    val (planOpt, planUnOpt) = plans
    val (countOpt, countUnOpt) = counts
    val (estCountOpt, estCountUnOpt) = estCounts
    val (cpuTimeOpt, cpuTimeUnOpt) = cpuTimes
    val (peakMemOpt, peakMemUnOpt) = peakMems

    val optStats = printAndWriteStats(
      fail,
      s"$resultsDir/opt",
      queryStr,
      numStmtGenerated,
      Array(),
      estCountOpt,
      planOpt,
      countOpt,
      startTimeOpt,
      endTimeOpt,
      cpuTimeOpt,
      peakMemOpt
    )

    val unOptStats = printAndWriteStats(
      fail,
      s"$resultsDir/unopt",
      queryStr,
      numStmtGenerated,
      Array(),
      estCountUnOpt,
      planUnOpt,
      countUnOpt,
      startTimeUnOpt,
      endTimeUnOpt,
      cpuTimeUnOpt,
      peakMemUnOpt
    )

    if(durationOpt > durationUnOpt || peakMemOpt > peakMemUnOpt || cpuTimeOpt > cpuTimeUnOpt) {
      val diffPeakMem = peakMemOpt - peakMemUnOpt
      val diffCpuTime = cpuTimeOpt - cpuTimeUnOpt

      val combinedStats =
        s"""
          |${makeDivider("Optimized Run Stats")}
          |$optStats
          |${makeDivider("Unoptimized Run Stats")}
          |$unOptStats
          |${makeDivider("Difference")}
          |(peakMemOpt - peakMemUnOpt): $diffPeakMem
          |(cpuTimeOpt - cpuTimeUnOpt): $diffCpuTime
          |${makeDivider("Query")}
          |$queryStr
          |""".stripMargin
      writeQueryToFile(combinedStats, s"${numStmtGenerated}", s"$resultsDir/opt-discrepancies/")
    }
  }

  def withOptimized[T](f: => T): T = {
    // Sets up all the configurations for the Catalyst optimizer
    val optConfigs = Seq(
      (SQLConf.CBO_ENABLED.key, "true"),
      (SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key, "true")
    )
    withSQLConf(optConfigs: _*) {
      f
    }
  }
  def withoutOptimized[T](excludedRules: String)(f: => T): T = {
    val nonOptConfigs = Seq((SQLConf.OPTIMIZER_EXCLUDED_RULES.key, excludedRules))
    withSQLConf(nonOptConfigs: _*) {
      f
    }
  }


  def prettyPrintStats(stats: CampaignStats): String = {
    val statsMap = stats.getMap
    val generated = stats.getGenerated
    val successful = statsMap.getOrElse("Success", "0").toInt + statsMap.getOrElse("MismatchException", "0").toInt
    val validityRate = (successful.toFloat / generated.toFloat) * 100

    val builder = new StringBuilder
    builder.append("=== STATS ===\n")
    builder.append("----- Details -----\n")
    statsMap.foreach { case (k, v) => builder.append(s"$k = $v\n") }
    builder.append("------ Summary -----\n")
    builder.append(f"Exiting after DFGs generated == $generated\n")
    builder.append(s"Validity Rate: ${validityRate}%\n")
    builder.append("=============\n")

    builder.toString()
  }

  def main(args: Array[String]): Unit = {

    // EXAMPLE ARGS: local[*] --output-location target/fuzz-tests-output --max-stmts 100
    // ON CLUSTER: local[*] --output-location target/fuzz-tests-output --max-stmts 100 --no-hive --tpcds-path tpcds-data/
    val master = args(0)
    val arguments = new FuzzerArguments(args.tail)
    val liveStatsAfter = 200
    val seed = arguments.seed.toInt
    val timeLimitSeconds = arguments.timeLimitSeconds.toInt
    val outputDir = new File(arguments.outputLocation)

    deleteDir(outputDir.getAbsolutePath)
    outputDir.mkdirs()

    val spark = setupSpark(master, arguments)

    val sparkOpt = spark.sessionState.optimizer
    val excludableRules = {
      val defaultRules = sparkOpt.defaultBatches.flatMap(_.rules.map(_.ruleName)).toSet
      val rules = defaultRules -- sparkOpt.nonExcludableRules.toSet
      debugPrint(
        s"""
           |excludedRules(${rules.size}):
           |  ${rules.mkString("\n  ")}
         """.stripMargin)
      rules
    }

    val excludedRules = excludableRules.mkString(",")


    setupEnv()

    if(!arguments.hive) {
      if (arguments.tpcdsDataPath.isEmpty) {
        sys.error("Hive disabled and --tpcds-path <path> not provided!")
      }
      println(s"Hive disabled, loading from tpcds data from path ${arguments.tpcdsDataPath}...")
      TpcdsTablesLoader.loadAll(spark, arguments.tpcdsDataPath)
      println("loaded successfully")
    }

    val targetTables = gatherTargetTables(spark)

    if (targetTables.isEmpty)
      throw new RuntimeException({"No tables found"})

    val catalog = dumpSparkCatalog(spark, targetTables)
    val sqlSmithSchema = sqlSmithApi.schemaInit(catalog, seed)

    var numStmtGenerated: Long = 0
    var optCov: Set[String] = Set()
    var unOptCov: Set[String] = Set()

    val startTime = System.currentTimeMillis()

    val stats: CampaignStats = new CampaignStats()
    val liveStatsDir = new File(outputDir, "live-stats")
    liveStatsDir.mkdirs()

    def getElapsedTimeSeconds: Long = {
      (System.currentTimeMillis() - startTime)/1000
    }

    var cumuCoverage: Set[String] = Set()

    while (getElapsedTimeSeconds < timeLimitSeconds) {
      val queryStr = generateSqlSmithQuery(sqlSmithSchema)
      numStmtGenerated += 1

      val (result, optDF, unOptDF) = try {

        val dfOpt = withOptimized {
          coverage.clear()
          val df = spark.sql(queryStr)
          df.explain(true)
          optCov = coverage.toSet
          df
        }

        val dfUnOpt = withoutOptimized(excludedRules) {
          coverage.clear()
          val df = spark.sql(queryStr)
          df.explain(true)
          unOptCov = coverage.toSet
          df
        }


        (OracleSystem.compareRuns(dfOpt, dfUnOpt), dfOpt, dfUnOpt)
      } catch {
        case NonFatal(e) => (e, null, null)
        case e =>
          sqlSmithApi.free(sqlSmithSchema)
          throw new RuntimeException(s"Fuzz testing stopped because: $e")
      }

      cumuCoverage = cumuCoverage.union(optCov.union(unOptCov))
      stats.setCumulativeCoverageIfChanged(cumuCoverage,stats.getGenerated,System.currentTimeMillis()-startTime)
      val ruleBranchesCovered = coverage.toSet.size
      val resultType = result.getClass.toString.split('.').last

      result match {
        case _: Success =>
          println(s"==== FUZZER ITERATION ${stats.getGenerated}=====")
          println(s"RESULT: $result")
          println(s"$ruleBranchesCovered")
        case _: MismatchException =>
          println(s"==== FUZZER ITERATION ${stats.getGenerated}====")
          println(s"RESULT: $result")
          println(s"$ruleBranchesCovered")
        case _ =>
          println(s"==== FUZZER ITERATION ${stats.getGenerated}====")
          println(s"RESULT: $resultType")
      }


      stats.updateWith(resultType) {
        case Some(existing) => Some((existing.toInt + 1).toString)
        case None => Some("1")
      }

      if(resultType != "ParseException") {

        // Create subdirectory inside outDir using the result value
        val resultSubDir = new File(outputDir, resultType)
        resultSubDir.mkdirs() // Creates the directory if it doesn't exist

        // Prepare output file in the result-named subdirectory
        val outFileName = s"g_${stats.getGenerated}-a_${stats.getAttempts}"
        val outFile = new File(resultSubDir, outFileName)

        // Write the fullSource to the file
        val writer = new FileWriter(outFile)
        writer.write(
          s"""
             |$queryStr
             |/*
             |===== UnOptimized Plan =====
             |${if(unOptDF != null) unOptDF.queryExecution.optimizedPlan else "null"}
             |===== Optimized Plan =======
             |${if(optDF != null) optDF.queryExecution.optimizedPlan else "null"}
             |
             |
             |Optimizer Branch Coverage: ${ruleBranchesCovered}
             |*/
             |""".stripMargin)
        writer.close()
      }

      stats.setGenerated(stats.getGenerated+1)

      if (stats.getGenerated % liveStatsAfter == 0) {
        writeLiveStats(liveStatsDir, stats, startTime)
      }

      if (optDF != null) {
        optDF.unpersist(false) // avoid disk spill
      }
      if (unOptDF != null) {
        unOptDF.unpersist(false)
      }
      System.gc()

    }

    writeLiveStats(liveStatsDir, stats, startTime)
    println(prettyPrintStats(stats))

    sqlSmithApi.free(sqlSmithSchema)
  }

  def writeLiveStats(outDir: File, stats: CampaignStats, campaignStartTime: Long): Unit = {
    val currentTime = System.currentTimeMillis()
    val elapsedSeconds = (currentTime - campaignStartTime) / 1000
    val liveStatsDir =s"${outDir}/live-stats"
    new File(liveStatsDir).mkdirs()
    val liveStatsFile = new File(liveStatsDir, s"live-stats-${stats.getGenerated}-${elapsedSeconds}s.txt")
    val liveStatsWriter = new FileWriter(liveStatsFile)
    stats.setElapsedSeconds(elapsedSeconds)
    liveStatsWriter.write(prettyPrintStats(stats))
    liveStatsWriter.close()
  }
}
