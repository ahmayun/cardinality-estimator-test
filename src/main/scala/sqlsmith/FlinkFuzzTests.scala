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

import fuzzer.core.CampaignStats
import fuzzer.core.MainFuzzer.deleteDir
import fuzzer.exceptions.{FuzzerException, MismatchException, ParserException, Success, TableException, ValidationException}
import fuzzer.oracle.OracleSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule.coverage
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import play.api.libs.json.{JsArray, JsBoolean, JsNull, JsNumber, JsObject, JsString, JsValue, Json}
import sqlsmith.loader.{SQLSmithLoader, Utils}
import utils.network.HttpUtils

import java.net.http.HttpClient
import java.io.{File, FileWriter, PrintWriter}
import java.sql.{Connection, DriverManager, Statement}
import java.time.Duration
import java.util.UUID
import scala.util.control.NonFatal
//import java.nio.file.Files


object FlinkFuzzTests {

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
    val fuzzerExceptions = stats.getMap("FuzzerException").toInt
    val successful = statsMap.getOrElse("Success", "0").toInt + statsMap.getOrElse("MismatchException", "0").toInt
    val validityRate = (successful.toFloat / (generated.toFloat-fuzzerExceptions)) * 100

    val builder = new StringBuilder
    builder.append("=== STATS ===\n")
    builder.append("----- Details -----\n")
    statsMap.foreach { case (k, v) => builder.append(s"$k = $v\n") }
    builder.append("------ Summary -----\n")
    builder.append(f"Exiting after DFGs generated == $generated\n")
    builder.append(s"Validity Rate: ${validityRate}% [$successful / ($generated-$fuzzerExceptions)]\n")
    builder.append("=============\n")

    builder.toString()
  }

  // Query execution result container
  case class QueryExecutionResult(
                                   result: Throwable,
                                   optimizedPlan: Option[String],
                                   unoptimizedPlan: Option[String],
                                   optimizedCoverage: Set[String],
                                   unoptimizedCoverage: Set[String],
                                   coverageCount: Int
                                 )

  // Abstract query executor trait
  trait QueryExecutor {
    def execute(query: String): QueryExecutionResult
    def cleanup(): Unit
  }

  // Spark implementation of QueryExecutor
  class SparkQueryExecutor(
                            spark: SparkSession,
                            excludedRules: String
                          ) extends QueryExecutor {

    override def execute(query: String): QueryExecutionResult = {
      try {
        var optCov: Set[String] = Set()
        var unOptCov: Set[String] = Set()

        val dfOpt = withOptimized {
          coverage.clear()
          val df = spark.sql(query)
          df.explain(true)
          optCov = coverage.toSet
          df
        }

        val dfUnOpt = withoutOptimized(excludedRules) {
          coverage.clear()
          val df = spark.sql(query)
          df.explain(true)
          unOptCov = coverage.toSet
          df
        }

        val result = OracleSystem.compareRuns(dfOpt, dfUnOpt)
        val optPlan = Option(dfOpt).map(_.queryExecution.optimizedPlan.toString)
        val unOptPlan = Option(dfUnOpt).map(_.queryExecution.optimizedPlan.toString)

        // Cleanup dataframes
        if (dfOpt != null) dfOpt.unpersist(false)
        if (dfUnOpt != null) dfUnOpt.unpersist(false)

        QueryExecutionResult(
          result = result,
          optimizedPlan = optPlan,
          unoptimizedPlan = unOptPlan,
          optimizedCoverage = optCov,
          unoptimizedCoverage = unOptCov,
          coverageCount = coverage.toSet.size
        )
      } catch {
        case NonFatal(e) =>
          QueryExecutionResult(e, None, None, Set(), Set(), 0)
        case e =>
          throw new RuntimeException(s"Fuzz testing stopped because: $e")
      }
    }

    override def cleanup(): Unit = {
      System.gc()
    }
  }

  // Example: Remote API executor
  class PyFlinkQueryExecutor(serverHost: String, serverPort: Int) extends QueryExecutor {

    // Create HTTP client and request
    val client: HttpClient = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(120))
      .build()

    loadData()

    private def parseResponse(responseBody: String): Option[JsValue] = {
      if (responseBody.nonEmpty) {
        Some(Json.parse(responseBody))
      } else {
        None
      }
    }

    private def jsValueToScala(jsValue: JsValue): Any = {
      jsValue match {
        case JsNull => null
        case JsBoolean(b) => b
        case JsNumber(n) => if (n.isValidInt) n.toInt else n.toDouble
        case JsString(s) => s
        case arr: JsArray => arr.value.map(jsValueToScala).toList
        case obj: JsObject => obj.value.map { case (k, v) => k -> jsValueToScala(v) }.toMap
      }
    }

    private def loadData(): Unit = {
      // Create JSON request
      val request = Json.obj(
        "message_type" -> "load_data",
        "catalog_name" -> "main"
      )

      val response = HttpUtils.postJson(client, request, serverHost, serverPort, timeoutSeconds = 120)

      val body = Json.parse(response.body())
      val success = (body \ "success").asOpt[Boolean]
      success match {
        case Some(true) =>
        case _ => throw new Exception("PyFlink server failed to load data!")
      }
    }

    private def jsonToMap(json: JsValue): Map[String, Any] = {
      json.as[Map[String, JsValue]].map { case (key, value) =>
        key -> jsValueToScala(value)
      }
    }
    private def createException(errorName: String, errorMessage: String, is_same: Boolean): Throwable = {

      errorName.trim() match {
        case _ if !is_same => new MismatchException(errorMessage)
        case "MismatchException" => new MismatchException(errorMessage)
        case "ValidationException" => new ValidationException(errorMessage)
        case "RuntimeException" => new RuntimeException(errorMessage)
        case "TableException" => new TableException(errorMessage)
        case "SqlParserException" => new ParserException(errorMessage)
        case "Success" => new Success("Generated query hit the optimizer")
        case _ => new Exception(errorMessage)
      }
    }

    private def mapToExecutionResult(responseMap: Map[String, Any]): QueryExecutionResult = {
      val same_output = responseMap("success").asInstanceOf[Boolean]
      val errorName = responseMap("error_name").asInstanceOf[String]
      val errorMessage = responseMap("error_message").asInstanceOf[String]
      val finalProgram = responseMap.getOrElse("final_program", "").asInstanceOf[String]
      val sourceWithResults = s"$finalProgram\n// RESULT: $errorName: $errorMessage"
      val result = createException(errorName, errorMessage, same_output)
      QueryExecutionResult(result, Some(sourceWithResults), None, Set(), Set(), 0)

    }

    override def execute(codeString: String): QueryExecutionResult = {

      // Create JSON request
      val jsonRequest = Json.obj(
        "message_type" -> "execute_code",
        "code" -> codeString,
        "code_type" -> "sql"
      )

      try {

        val response = HttpUtils.postJson(client, jsonRequest, serverHost, serverPort)

        println(s"HTTP Response Status: ${response.statusCode()}")
        val responseBody = response.body()

        val responseJsonOpt = parseResponse(responseBody)

        val responseJson = responseJsonOpt match {
          case None => throw new Exception("response could not be parsed")
          case Some(responseJson) => responseJson
        }

        val responseMap = jsonToMap(responseJson)
        //    println(prettyPrintMap(responseMap))

        mapToExecutionResult(responseMap)
      } catch {
        case NonFatal(e) =>
          QueryExecutionResult(new FuzzerException(e), None, None, Set(), Set(), 0)
        case e =>
          throw new RuntimeException(s"Fuzz testing stopped because: $e")
      }

    }

    override def cleanup(): Unit = {
      // Cleanup resources if needed
    }

  }

  // Main function - now executor-agnostic
  def main(args: Array[String]): Unit = {
    val master = args(0)
    val arguments = new FuzzerArguments(args.tail)
    val outputDir = prepareOutputDirectory(arguments.outputLocation)

    // Setup phase (can be made pluggable too)
    val spark = setupSpark(master, arguments)
    val excludedRules = getExcludedRules(spark)
    setupEnv()
    loadDataIfNeeded(spark, arguments)

    val targetTables = gatherTargetTables(spark)
    validateTables(targetTables)

    val catalog = dumpSparkCatalog(spark, targetTables)
    val sqlSmithSchema = sqlSmithApi.schemaInit(catalog, arguments.seed.toInt)

    val stats = initializeStats(arguments, outputDir)

    // Create executor (can swap implementations here)
//    val executor: QueryExecutor = new SparkQueryExecutor(spark, excludedRules)
    val executor: QueryExecutor = new PyFlinkQueryExecutor("localhost", 8888)

    try {
      runFuzzingCampaign(
        executor,
        sqlSmithSchema,
        outputDir,
        stats,
        arguments.timeLimitSeconds.toInt
      )
    } finally {
      sqlSmithApi.free(sqlSmithSchema)
    }

    println(prettyPrintStats(stats))
  }

  private def prepareOutputDirectory(location: String): File = {
    val baseDir = new File(location)
    baseDir.mkdirs()

    // Create timestamped subdirectory
    val timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new java.util.Date())
    val timestampedDir = new File(baseDir, timestamp)
    timestampedDir.mkdirs()

    timestampedDir
  }

  private def getExcludedRules(spark: SparkSession): String = {
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
    excludableRules.mkString(",")
  }

  private def loadDataIfNeeded(spark: SparkSession, arguments: FuzzerArguments): Unit = {
    if (!arguments.hive) {
      if (arguments.tpcdsDataPath.isEmpty) {
        sys.error("Hive disabled and --tpcds-path <path> not provided!")
      }
      println(s"Hive disabled, loading from tpcds data from path ${arguments.tpcdsDataPath}...")
      TpcdsTablesLoader.loadAll(spark, arguments.tpcdsDataPath)
      println("loaded successfully")
    }
  }

  private def validateTables(tables: Seq[Table]): Unit = {
    if (tables.isEmpty) {
      throw new RuntimeException("No tables found")
    }
  }

  private def initializeStats(arguments: FuzzerArguments, outputDir: File): CampaignStats = {
    val stats = new CampaignStats()
    stats.setSeed(arguments.seed.toInt)
    val liveStatsDir = new File(outputDir, "live-stats")
    liveStatsDir.mkdirs()
    stats
  }

  // Now executor-agnostic - works with any QueryExecutor implementation
  private def runFuzzingCampaign(
                                  executor: QueryExecutor,
                                  sqlSmithSchema: Long,
                                  outputDir: File,
                                  stats: CampaignStats,
                                  timeLimitSeconds: Int
                                ): Unit = {
    val liveStatsAfter = 200
    val startTime = System.currentTimeMillis()
    val liveStatsDir = new File(outputDir, "live-stats")
    var cumuCoverage: Set[String] = Set()

    def getElapsedTimeSeconds: Long = {
      (System.currentTimeMillis() - startTime) / 1000
    }

    while (getElapsedTimeSeconds < timeLimitSeconds) {
      val queryStr = generateSqlSmithQuery(sqlSmithSchema)

      // Execute query through the pluggable executor
      val executionResult = executor.execute(queryStr)

      cumuCoverage = updateCoverage(
        cumuCoverage,
        executionResult.optimizedCoverage,
        executionResult.unoptimizedCoverage,
        stats,
        startTime
      )

      val resultType = classifyResult(executionResult.result)

      logIterationResult(stats, executionResult.result, resultType, executionResult.coverageCount)
      updateStatsCounter(stats, resultType)

      if (resultType != "ParseException") {
        writeQueryArtifact(
          outputDir,
          stats,
          queryStr,
          executionResult.optimizedPlan,
          executionResult.unoptimizedPlan,
          executionResult.coverageCount,
          resultType,
          executionResult.result
        )
      }

      stats.setGenerated(stats.getGenerated + 1)

      if (stats.getGenerated % liveStatsAfter == 0) {
        writeLiveStats(liveStatsDir, stats, startTime)
      }

      executor.cleanup()
    }

    writeLiveStats(liveStatsDir, stats, startTime)
  }

  private def updateCoverage(
                              cumuCoverage: Set[String],
                              optCov: Set[String],
                              unOptCov: Set[String],
                              stats: CampaignStats,
                              startTime: Long
                            ): Set[String] = {
    val updated = cumuCoverage.union(optCov.union(unOptCov))
    stats.setCumulativeCoverageIfChanged(
      updated,
      stats.getGenerated,
      System.currentTimeMillis() - startTime
    )
    updated
  }

  private def classifyResult(result: Throwable): String = {
    result.getClass.toString.split('.').last
  }

  private def logIterationResult(
                                  stats: CampaignStats,
                                  result: Throwable,
                                  resultType: String,
                                  ruleBranchesCovered: Int
                                ): Unit = {
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
  }

  private def updateStatsCounter(stats: CampaignStats, resultType: String): Unit = {
    stats.updateWith(resultType) {
      case Some(existing) => Some((existing.toInt + 1).toString)
      case None => Some("1")
    }
  }

  private def writeQueryArtifact(
                                  outputDir: File,
                                  stats: CampaignStats,
                                  queryStr: String,
                                  optimizedPlan: Option[String],
                                  unoptimizedPlan: Option[String],
                                  ruleBranchesCovered: Int,
                                  resultType: String,
                                  result: Throwable
                                ): Unit = {
    // Extract exception name from the result object
    val exceptionName = resultType

    val resultSubDir = new File(outputDir, exceptionName)
    resultSubDir.mkdirs()

    val outFileName = s"g_${stats.getGenerated}-a_${stats.getAttempts}"
    val outFile = new File(resultSubDir, outFileName)

    val writer = new FileWriter(outFile)
    writer.write(
      s"""
         |$queryStr
         |/*
         |===== UnOptimized Plan =====
         |${unoptimizedPlan.getOrElse("null")}
         |===== Optimized Plan =======
         |${optimizedPlan.getOrElse("null")}
         |
         |RESULT:
         |    NAME: $resultType
         |    DETAILS: ${result.getMessage}
         |Optimizer Branch Coverage: ${ruleBranchesCovered}
         |*/
         |""".stripMargin)
    writer.close()
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
