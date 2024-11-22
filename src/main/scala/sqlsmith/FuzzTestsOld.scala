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

import com.google.common.io.Files
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types._
import sqlsmith.loader.{SQLSmithLoader, Utils}

import java.io.{File, PrintWriter}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, Statement}
import java.util.UUID
import scala.collection.mutable
import scala.util.control.NonFatal
//import java.nio.file.Files


object FuzzTestsOld {

  private lazy val sqlSmithApi = SQLSmithLoader.loadApi()

  // Loads the SQLite JDBC driver
//  Class.forName("org.sqlite.JDBC")

  private def debugPrint(s: String): Unit = {
    // scalastyle:off println
    println(s)
    // scalastyle:on println
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

  def main(args: Array[String]): Unit = {
    val arguments = new FuzzerArguments(args.tail)
    val outputDir = new File(arguments.outputLocation)
    if (!outputDir.exists()) {
      outputDir.mkdir()
    }

    val results_dir = arguments.outputLocation

    // TODO: Makes this code independent from the Spark version
    val spark = SparkSession.builder()
      .appName("FuzzTest")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master(args(0))
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    Class.forName("org.sqlite.JDBC")

    val targetTables = {
      spark.catalog.listDatabases().collect().flatMap(db => spark.catalog.listTables(db.name).collect())
    }.filter(_.database == "main") // sqlsmith will only reference main so others not needed
    println(targetTables.mkString("Array(", ", ", ")"))
//    println(spark.catalog.listCatalogs().collect().mkString("Array(", ", ", ")"))
//    println(spark.catalog.listTables("person").collect().mkString("Array(", ", ", ")"))
//
//    sys.exit(0)

//    targetTables.foreach { t =>
//      println(s"Analyzing ${t.database}.${t.name}...")
//      spark.sql(s"ANALYZE TABLE ${t.database}.${t.name} COMPUTE STATISTICS")
//    }





    //    val sparkOpt = spark.sessionState.optimizer
//    val excludableRules = {
//      val defaultRules = sparkOpt.defaultBatches.flatMap(_.rules.map(_.ruleName)).toSet
//      val rules = defaultRules -- sparkOpt.nonExcludableRules.toSet
//      debugPrint(
//        s"""
//           |excludedRules(${rules.size}):
//           |  ${rules.mkString("\n  ")}
//         """.stripMargin)
//      rules
//    }

//    def withOptimized[T](f: => T): T = {
//      // Sets up all the configurations for the Catalyst optimizer
//      val optConfigs = Seq(
//        (SQLConf.CBO_ENABLED.key, "true"),
//        (SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key, "true")
//      )
//      withSQLConf(optConfigs: _*) {
//        f
//      }
//    }
//
//    val excludedRules = excludableRules.mkString(",")
//    def withoutOptimized[T](f: => T): T = {
//      val nonOptConfigs = Seq((SQLConf.OPTIMIZER_EXCLUDED_RULES.key, excludedRules))
//      withSQLConf(nonOptConfigs: _*) {
//        f
//      }
//    }
    def makeDivider(title: String = ""): String = {
      s"============================${title}========================================"
    }

    if (targetTables.nonEmpty) {
      val sqlSmithSchema = sqlSmithApi.schemaInit(
        dumpSparkCatalog(spark, targetTables), arguments.seed.toInt)
      var numStmtGenerated: Long = 0
      val errStore = mutable.Map[String, Long]()
      var numLogicErrors: Long = 0

      def dumpTestingStats(): Unit = debugPrint({
        val numErrors = errStore.values.sum
        val numValidStmts = numStmtGenerated - numErrors
        val numValidTestRatio = (numValidStmts + 0.0) / numStmtGenerated
        val errStats = errStore.toSeq.sortBy(_._2).reverse.map { case (e, n) => s"# of $e: $n" }
        s"""Fuzz testing statistics:
           |  valid test ratio: $numValidTestRatio ($numValidStmts/$numStmtGenerated)
           |  # of found logic errors: $numLogicErrors
           |  ${errStats.mkString("\n  ")}
         """.stripMargin
      })

      // Dumps the stats when doing shutdown
      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run(): Unit = dumpTestingStats()
      })

      val blackListExceptionMsgs = Seq(
        "Expressions referencing the outer query are not supported outside of WHERE/HAVING clauses"
      )

      try {
        val maxStmts = arguments.maxStmts.toLong
        val isInfinite = maxStmts == 0
        while (isInfinite || numStmtGenerated < maxStmts) {
          val sqlFuzz = sqlSmithApi.getSQLFuzz(sqlSmithSchema)
          numStmtGenerated += 1

          // Prints the stats periodically
          if (numStmtGenerated % 1000 == 0) {
            dumpTestingStats()
          }

//          println(s"GENERATED QUERY:\n$sqlFuzz")
//          println("===============================")

          var status = "ERR_OTHER" // assume generic failure

//          spark.sparkContext.addSparkListener(memoryUsageListener)
//          spark.sparkContext.addSparkListener(cpuTimeListener)
//          spark.sparkContext.addSparkListener(memoryUsageListener)

          try {

            val estCount = spark.sql(sqlFuzz).queryExecution.optimizedPlan.stats.rowCount match {
              case Some(c) => c
              case None =>
//                println("None Estimate Query")
//                println(sqlFuzz)
                BigInt(-1)
            }

            val startTime = System.nanoTime()
            val df = spark.sql(sqlFuzz)
            val actualCount = df.count()
            val endTime = System.nanoTime()
            val duration = (endTime - startTime) / 1e9  // Convert to seconds if using nanoTime, or to milliseconds if using currentTimeMillis

            val sign = if (estCount-actualCount >= 0) "+" else "-"
            val absDiff = (estCount-actualCount).abs

            println(s"Estimated count: $estCount")
            println(s"Actual count: $actualCount")
            println(s"Abs diff: $absDiff")
            println(s"Exec time: $duration seconds")
            println(s"Time diff: TBC")
            val metrics = metricComputers.map {f =>
              val (name, value) = f(sqlFuzz)
              val s = s"$name:\n$value"
              println(s)
              s
            }

            val output =
              s"""
                |${makeDivider("STATS")}
                |Actual Count: $actualCount
                |Estimated Count: ${if (estCount < 0) "No Estimate" else estCount}
                |Abs diff: $absDiff
                |Exec time: $duration seconds
                |${metrics.mkString("\n")}
                |${makeDivider("QUERY")}
                |$sqlFuzz
                |${makeDivider("PLAN")}
                |${df.queryExecution.toString()}
                |""".stripMargin

            status = s"PASS"
            if(estCount < 0) {
              writeQueryToFile(output, s"${numStmtGenerated}_${actualCount}", s"$results_dir/queries/$status/NO_ESTIMATES")
            } else if (absDiff > 0) {
              writeQueryToFile(output, s"${numStmtGenerated}_${sign}${absDiff}", s"$results_dir/queries/$status/DIFFERENT_ESTIMATES")
            } else {
              writeQueryToFile(output, s"${numStmtGenerated}", s"$results_dir/queries/$status/ACCURATE_ESTIMATES")
            }

            println(makeDivider())

//            println(sqlFuzz)
//            println("--------------------------")

//             val optNum = withOptimized { spark.sql(sqlFuzz).count() }
//             val nonOptNum = withoutOptimized { spark.sql(sqlFuzz).count() }
//             // TODO: We need more strict checks for the answers
//             if (optNum != nonOptNum) {
//               Files.write(
//                 s"""##### query: optNum($optNum) != nonOptNum($nonOptNum) #####
//                    |$sqlFuzz
//                  """.stripMargin,
//                 new File(outputDir, s"$numLogicErrors.log"),
//                 StandardCharsets.UTF_8
//               )
//               numLogicErrors += 1
//             }
          } catch {
            case NonFatal(e) =>
              e match {
                case _: org.apache.spark.sql.catalyst.parser.ParseException =>
                  status = "ERR_PARSE"
                case _: org.apache.spark.sql.AnalysisException =>
                  status = "ERR_ANLYS"
                case _ =>
                  status = "ERR_OTHER"
              }
              // If enabled, outputs an exception log
              val exceptionName = e.getClass.getSimpleName
              if (arguments.loggingExceptionsEnabled) {
                val exceptionMsg = e.getMessage
                if (!blackListExceptionMsgs.exists(exceptionMsg.contains)) {
                  val exceptionLoggingDir = new File(outputDir, exceptionName)
                  if (!exceptionLoggingDir.exists()) {
                    exceptionLoggingDir.mkdir()
                  }
                  Files.write(
                    s"""##### ${e.getClass.getCanonicalName} #####
                       |$exceptionMsg
                       |Generated SQL Fuzz:
                       |$sqlFuzz
                     """.stripMargin,
                    new File(exceptionLoggingDir, s"$numStmtGenerated.log"),
                    StandardCharsets.UTF_8
                  )
                }
              }

              // Finally, updates exception stats
              val curVal = errStore.getOrElseUpdate(exceptionName, 0)
              errStore.update(exceptionName, curVal + 1)
              writeQueryToFile(s"${sqlFuzz}\n${makeDivider("ERROR")}\n$e", s"${numStmtGenerated}", s"$results_dir/queries/$status")
            case e =>
              throw new RuntimeException(s"Fuzz testing stopped because: $e")
          }
        }
      } finally {
        sqlSmithApi.free(sqlSmithSchema)
      }
    } else {
      throw new RuntimeException({
        val catalogImpl = spark.conf.get(StaticSQLConf.CATALOG_IMPLEMENTATION.key)
        s"Table entries not found in the $catalogImpl catalog"
      })
    }
  }
}
