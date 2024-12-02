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

import java.io.File
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, Statement}
import java.util.UUID
import scala.collection.mutable
import scala.util.control.NonFatal
import com.google.common.io.Files
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types._
import sqlsmith.loader.SQLSmithLoader
import sqlsmith.loader.Utils

import java.io.PrintWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical._
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

  private def dumpSparkCatalog(spark: SparkSession, tables: Seq[Table]): String = {
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
    (keys, values).zipped.foreach { (k, v) =>
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

  def setupSpark(master: String): SparkSession = {
    val spark = SparkSession.builder()
      .appName("FuzzTest")
//      .config("spark.sql.cbo.enabled", "true")
//      .config("spark.sql.cbo.joinReorder.enabled", "true")
//      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
//      .config("spark.sql.statistics.histogram.enabled", "true")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()
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
                      counts: (Long, Long),
                      estCounts: (BigInt, BigInt),
                      startTimes: (Long, Long),
                      endTimes: (Long, Long),
                      cpuTimes: (Long, Long),
                      peakMems: (Long, Long)): Unit = {


    val (startTimeOpt, startTimeUnOpt) = startTimes
    val (endTimeOpt, endTimeUnOpt) = endTimes
    val (durationOpt, durationUnOpt) = ((endTimeOpt-startTimeOpt) / 1e9, (endTimeUnOpt-startTimeUnOpt) / 1e9)
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
  def main(args: Array[String]): Unit = {
    val master = args(0)
    val arguments = new FuzzerArguments(args.tail)
    val maxStmts = arguments.maxStmts.toLong
    val isInfinite = maxStmts == 0
    val seed = arguments.seed.toInt
    val outputDir = new File(arguments.outputLocation)
    if (!outputDir.exists()) {
      outputDir.mkdir()
    }
    val resultsDir = arguments.outputLocation

    val spark = setupSpark(master)

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

    val targetTables = gatherTargetTables(spark)

    if (targetTables.isEmpty)
      throw new RuntimeException({"No tables found"})

    val catalog = dumpSparkCatalog(spark, targetTables)
    val sqlSmithSchema = sqlSmithApi.schemaInit(catalog, seed)

    var numStmtGenerated: Long = 0
    val errStore = mutable.Map[String, Long]()

    def dumpTestingStats(): Unit = debugPrint({
      val numErrors = errStore.values.sum
      val numValidStmts = numStmtGenerated - numErrors
      val numValidTestRatio = (numValidStmts + 0.0) / numStmtGenerated
      val errStats = errStore.toSeq.sortBy(_._2).reverse.map { case (e, n) => s"# of $e: $n" }
      s"""Fuzz testing statistics:
         |  valid test ratio: $numValidTestRatio ($numValidStmts/$numStmtGenerated)
         |  ${errStats.mkString("\n  ")}
       """.stripMargin
    })

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = dumpTestingStats()
    })

    // ========= LISTENERS ========================

    class CpuTimeListener extends SparkListener {
      var cpuTime: Long = 0
      var peakMemory: Long = 0

      override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
        val stageInfo = stageCompleted.stageInfo
        val taskMetrics = stageInfo.taskMetrics
        val executorCpuTime = taskMetrics.executorCpuTime
        val peakMem = taskMetrics.peakExecutionMemory
        val status = stageInfo.failureReason match {
          case None => "Success"
          case Some(_) => "Failed"
        }
        cpuTime += executorCpuTime
        peakMemory = math.max(peakMem, peakMemory)
//        println(s"Stage ${stageInfo.stageId} [${status}] - CPU time: ${executorCpuTime}, Peak Mem: ${peakMem} (globalPeak: ${peakMemory})")
      }
    }
    // ============================================

    while (isInfinite || numStmtGenerated < maxStmts) {
      val queryStr = generateSqlSmithQuery(sqlSmithSchema)
      numStmtGenerated += 1

      if (numStmtGenerated % 1000 == 0) {
        dumpTestingStats()
      }

      var status = "ERR_OTHER"

      try {
        // warm up
        spark.sql(queryStr).count()
        spark.catalog.clearCache()
        spark.sqlContext.clearCache()



        val (countOpt, estCountOpt, startTimeOpt, endTimeOpt, cpuTimeOpt, peakMemOpt) = withOptimized {
          val optListener = new CpuTimeListener()
          spark.sparkContext.addSparkListener(optListener)
          val st = System.nanoTime()
          val ct = spark.sql(queryStr).count()
          val et = System.nanoTime()
          val estCount = getEstimatedCount(spark, queryStr)
          val cpuTime = optListener.cpuTime
          val peakMem = optListener.peakMemory
          val ret = (ct, estCount, st, et, cpuTime, peakMem)
          spark.sparkContext.removeSparkListener(optListener)
          ret
        }

        val (countUnOpt, estCountUnOpt, startTimeUnOpt, endTimeUnOpt, cpuTimeUnOpt, peakMemUnOpt) = withoutOptimized(excludedRules) {
          val UnOptListener = new CpuTimeListener()
          spark.sparkContext.addSparkListener(UnOptListener)
          val st = System.nanoTime()
          val ct = spark.sql(queryStr).count()
          val et = System.nanoTime()
          val estCount = getEstimatedCount(spark, queryStr)
          val cpuTime = UnOptListener.cpuTime
          val peakMem = UnOptListener.peakMemory
          val ret = (ct, estCount, st, et, cpuTime, peakMem)
          spark.sparkContext.removeSparkListener(UnOptListener)
          ret
        }


        compareAndWrite(
          fail=false,
          resultsDir,
          queryStr,
          numStmtGenerated,
          (countOpt, countUnOpt),
          (estCountOpt, estCountUnOpt),
          (startTimeOpt, startTimeUnOpt),
          (endTimeOpt, endTimeUnOpt),
          (cpuTimeOpt, cpuTimeUnOpt),
          (peakMemOpt, peakMemUnOpt)
        )

      } catch {
        case NonFatal(e) =>
          status = determinErrorType(e)
          val exceptionName = e.getClass.getSimpleName
          val curVal = errStore.getOrElseUpdate(exceptionName, 0)
          errStore.update(exceptionName, curVal + 1)
          writeQueryToFile(s"${queryStr}\n${makeDivider("ERROR")}\n$e", s"${numStmtGenerated}", s"$resultsDir/queries/$status")
        case e =>
          sqlSmithApi.free(sqlSmithSchema)
          throw new RuntimeException(s"Fuzz testing stopped because: $e")
      }
    }

    sqlSmithApi.free(sqlSmithSchema)
  }
}
