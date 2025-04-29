package lancer

import fuzzer.core.CampaignStats
import fuzzer.core.MainFuzzer.{constructCombinedFileContents, deleteDir, writeLiveStats}
import fuzzer.exceptions.{MismatchException, Success}
import fuzzer.global.FuzzerConfig
import fuzzer.oracle.OracleSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule.coverage
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import sqlsmith.FuzzTests.{setupSpark, writeLiveStats}
import sqlsmith.FuzzerArguments

import java.io.{File, FileWriter}

object RunSQLancerScala {
  var spark: SparkSession = SparkSession.builder()
    .appName("FuzzTest")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.ui.enabled", "false")
    .master("local[*]")
    .getOrCreate()

  var cumuCoverage: Set[String] = Set()
  val stats: CampaignStats = new CampaignStats()
  val startTime: Long = System.currentTimeMillis()
  var master: String = ""
  var arguments: FuzzerArguments = null
  var liveStatsAfter: Int = 2000
  var seed: Int = 0
  var timeLimitSeconds: Long = 0
  var outputDir: File = null
  var liveStatsDir: File = null

  def setupDB(): Unit = {

    val spark = this.spark
    import spark.implicits._

    // Create vt0: equivalent to FTS5 table (c0 UNINDEXED)
    val vt0Schema = StructType(Seq(
      StructField("c0", StringType, nullable = true)
    ))

    // Initialize vt0 with dummy data (based on your inserts)
    val vt0Data = Seq(
      (null: String),
      ("1535245655"),
      (""),
      ("vu.ll{>"),
      ("742839790"),
      ("-1577331487"),
      ("oE%d\rM"),
      (null: String),
      (null: String),
      ("x(g"),
      ("t扏5"),
      ("automerge"),
      (null: String)
    ).toDF("c0")

    // Register vt0 as a temporary view
    vt0Data.createOrReplaceTempView("vt0")

    // Create t1: equivalent to TEMP table (c0 BLOB CHECK (constant))
    val t1Schema = StructType(Seq(
      StructField("c0", StringType, nullable = true)  // using StringType to mimic flexible blob/numeric/text in SQLite
    ))

    // Initialize t1 with dummy data (based on your inserts)
    val t1Data = Seq(
      ("n厗"),
      ("躆2"),
      ("ixN!ﵗ[*"),
      ("-1577331487"),
      ("1.535245655E9"),
      (null: String),
      ("0.5821716023398449"),
      ("0.9396699083242657"),
      (null: String),
      ("x'475d'"),
      ("x'6658'"),
      ("x'3198'"),
      ("-1.577331487E9"),
      ("0xffffffffa1fbd4e1"),
      ("0xffffffffb52f3b40"),
      ("x'7ceb'"),
      ("x''")
    ).toDF("c0")

    // Register t1 as a temporary view
    t1Data.createOrReplaceTempView("t1")

    // Create view v0 similar to the complex one you had
    spark.sql("""
  CREATE OR REPLACE TEMP VIEW v0 AS
  SELECT
    RANK() OVER (ORDER BY vt0.c0) AS c0,
    LOWER(AVG(CAST(vt0.c0 AS DOUBLE)) OVER ()) AS c1,
    CASE
      WHEN NOT (vt0.c0 IS NULL OR t1.c0 IS NULL) THEN 1
      ELSE 0
    END AS c2
  FROM vt0
  CROSS JOIN t1
""")
  }

  def main(args: Array[String]): Unit = {

    master = "local[*]"
//    arguments = // new FuzzerArguments(args.tail)
    seed = 0// arguments.seed.toInt
    timeLimitSeconds = 15 // arguments.timeLimitSeconds.toLong
    outputDir = new File("target/sqlancer") // new File(arguments.outputLocation)
    liveStatsDir = new File(outputDir, "live-stats")
    deleteDir(outputDir.getAbsolutePath)
    outputDir.mkdirs()

    setupDB()
    System.exit(lancer.RunSQLancer.executeMain(
      "--num-threads", "1",
      "sqlite3",
      "--oracle", "NoREC"
      //TODO: Set seed from config
      //TODO: Somehow limit time
    ))
  }



  def printQueries(optQuery: String, unoptQuery: String): Unit = {
    coverage.clear()
    val (result, (optResult, _), (unOptResult, _)) = OracleSystem.checkSqlancer(spark, optQuery, unoptQuery)
    cumuCoverage = cumuCoverage.union(coverage.toSet)
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
           |/*
           |
           |Opt Query:
           |$optQuery
           |
           |Unopt Query:
           |$unoptQuery
           |
           |Result:
           |$result
           |Optimizer Branch Coverage: ${ruleBranchesCovered}
           |*/
           |""".stripMargin)
      writer.close()
    }

    stats.setGenerated(stats.getGenerated+1)

    if (stats.getGenerated % liveStatsAfter == 0) {
      writeLiveStats(liveStatsDir, stats, startTime)
    }

    val elapsed = (System.currentTimeMillis() - startTime) /1000
    if (elapsed >= timeLimitSeconds) {
      writeLiveStats(liveStatsDir, stats, startTime)
      System.exit(0)
    }
  }

}
