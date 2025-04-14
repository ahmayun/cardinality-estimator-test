package fuzzer.core
import fuzzer.data.tables.Examples.tpcdsTables
import fuzzer.data.tables.{ColumnMetadata, TableMetadata}
import fuzzer.oracle.OracleSystem
import fuzzer.exceptions.{ImpossibleDFGException, MismatchException, Success}
import fuzzer.generation.Graph2Code.{constructDFG, dag2Scala}
import fuzzer.global.FuzzerConfig
import fuzzer.graph.{DAGParser, DFOperator, Graph}
import fuzzer.templates.Harness
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.yaml.snakeyaml.Yaml
import play.api.libs.json.JsValue
import utils.json.JsonReader

import scala.sys.process._
import utils.Random

import java.io.{File, FileWriter}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks.{break, breakable}


object MainFuzzer {
  def deleteDir(path: String): Unit = {
    val cmd = s"rm -rf ${path}"
    val exitCode = cmd.!
    if (exitCode != 0) {
      println(s"Failed to delete $path")
    }
  }

//  private def isInvalidDFG(dag: Graph[DFOperator], specScala: JsValue, hardcodedTables: List[TableMetadata]): Boolean = {
//    dag.nodes.exists(_.getInDegree > 2)
//  }

  private def isInvalidDFG(dag: Graph[DFOperator], specScala: JsValue, hardcodedTables: List[TableMetadata]): (Boolean, String) = {
    dag match {
      case _ if dag.nodes.exists(_.getInDegree > 2) =>
        (true, "Has a node with in-degree > 2.")
      case _ if dag.getSinkNodes.length > 1 =>
        (true, "DAG has more than one sink.")
      case _ if dag.getSinkNodes.head.parents.length > 1 =>
        (false, "Sink has more than one parent.") // Not ideal, but can let this slide.
      case _ =>
        (false, "")
    }

  }

  def genTempConfigWithNewSeed(yamlFile: String): String = {
    val yaml = new Yaml()
    val newSeed = Random.nextInt(Int.MaxValue)
    val inputStream = new java.io.FileInputStream(new File(yamlFile))
    val data = yaml.load[java.util.Map[String, Object]](inputStream).asScala
    data.update("Seed", Integer.valueOf(newSeed))
    val outputPath = "/tmp/runtime-config.yaml"
    val writer = new FileWriter(outputPath)
    yaml.dump(data.asJava, writer)
    writer.close()
    outputPath
  }


  private def prettyPrintStats(stats: mutable.Map[String, Int]): String = {
    val generated = stats("generated")
    val attempts = stats("attempts")
    val successful = stats.getOrElse("Success", 0)

    val builder = new StringBuilder
    builder.append("=== STATS ===\n")
    builder.append("----- Details -----\n")
    stats.foreach { case (k, v) => builder.append(s"$k = $v\n") }
    builder.append("------ Summary -----\n")
    builder.append(f"Exiting after DFGs generated == $generated\n")
    val tpDag2Dfg = (generated.toFloat / attempts.toFloat) * 100
    val tpDag2Valid = (successful.toFloat / attempts.toFloat) * 100
    val tpDfg2Valid = (successful.toFloat / generated.toFloat) * 100
    builder.append(f"Throughput DAG -> DFG: $generated/$attempts ($tpDag2Dfg%.2f%%)\n")
    builder.append(f"Throughput DAG -> Valid: $successful/$attempts ($tpDag2Valid%.2f%%)\n")
    builder.append(f"Throughput DFG -> Valid: $successful/$generated ($tpDfg2Valid%.2f%%)\n")
    builder.append("=============\n")

    builder.toString()
  }


  def main(args: Array[String]): Unit = {
    // Hard-coded table metadata for demonstration
    val config = if (!args.isEmpty) FuzzerConfig.fromJsonFile(args(0)) else FuzzerConfig.getDefault
    Random.setSeed(config.seed)


    deleteDir(config.dagGenDir)
    deleteDir(config.outDir)

    val stats = mutable.Map[String, Int](
      "attempts" -> 0,
      "generated" -> 0,
      "dag-batch" -> 0
    )


    val sparkSession = SparkSession.builder()
      .appName("Fuzzer")
      .master(config.master)
//      .enableHiveSupport()
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    fuzzer.global.State.sparkOption = Some(sparkSession)

    val specScala = JsonReader.readJsonFile("specs/spark-scala-no-action-full.json")

    new File(config.outDir).mkdirs()

    val startTime = System.currentTimeMillis()

    def stop: Boolean = {
      val elapsed = (System.currentTimeMillis() - startTime) / 1000
      if (config.exitAfterNSuccesses) {
        config.exitAfterNSuccesses && stats("generated") == config.N
      } else {
        elapsed >= config.timeLimitSec
      }
    }

    def generateDAGs: File = {
      val generateCmd = s"./dag-gen/venv/bin/python dag-gen/run_generator.py -c ${genTempConfigWithNewSeed("dag-gen/sample_config/dfg-config.yaml")}" // dag-gen/sample_config/dfg-config.yaml
      stats("dag-batch") += 1
      val exitCode = generateCmd.!
      if (exitCode != 0) {
        println(s"Warning: DAG generation command failed with exit code $exitCode")
        sys.exit(-1)
      }

      val dagFolder = new File(config.dagGenDir)
      if (!dagFolder.exists() || !dagFolder.isDirectory) {
        println("Warning: 'DAGs' folder not found or not a directory. Exiting.")
        sys.exit(-1)
      }
      dagFolder
    }

    // Main fuzzer loop
    while (!stop) {

      val dagFolder = generateDAGs
      val yamlFiles = dagFolder
        .listFiles()
        .filter(f => f.isFile && f.getName.startsWith("dag") && f.getName.endsWith(".yaml"))
        .take(config.d)

      breakable {
        for (yamlFile <- yamlFiles) {
          if (stop)
            break

          val dagName = yamlFile.getName

          try {
            val dag = DAGParser.parseYamlFile(yamlFile.getAbsolutePath, map => DFOperator.fromMap(map))

            val (isInvalid, message) = isInvalidDFG(dag, specScala, tpcdsTables)
            if (isInvalid) {
              throw new ImpossibleDFGException(s"Impossible to convert DAG to DFG. $message")
            }

            breakable {
              for (i <- 1 to config.p) {
                if (stop)
                  break

                fuzzer.global.State.iteration += 1
                try {
                  val selectedTables = Random.shuffle(tpcdsTables).take(dag.getSourceNodes.length).toList
                  val dfg = constructDFG(dag, specScala, selectedTables)
                  val generatedSource = dfg.generateCode(dag2Scala(specScala))

                  val (result, (optResult, fullSourceOpt), (unOptResult, fullSourceUnOpt)) = OracleSystem.checkOneGo(generatedSource.toString)
                  val resultType = result.getClass.toString.split('.').last

                  result match {
                    case _: Success =>
                      println(s"==== FUZZER ITERATION ${fuzzer.global.State.iteration}====")
                      println(s"RESULT: $result")
                    case _: MismatchException =>
                      println(s"==== FUZZER ITERATION ${fuzzer.global.State.iteration}====")
                      println(s"RESULT: $result")
                    case _ =>
                      println(s"==== FUZZER ITERATION ${fuzzer.global.State.iteration}====")
                      println(s"RESULT: $resultType")
                  }

                  val combinedSourceWithResults = constructCombinedFileContents(result, optResult, unOptResult, fullSourceOpt, fullSourceUnOpt)

                  stats.updateWith(resultType) {
                    case Some(existing) => Some(existing + 1)
                    case None => Some(1)
                  }

                  // Create subdirectory inside outDir using the result value
                  val resultSubDir = new File(config.outDir, resultType)
                  resultSubDir.mkdirs() // Creates the directory if it doesn't exist

                  // Prepare output file in the result-named subdirectory
                  val outFileName = s"g_${stats("generated")}-a_${stats("attempts")}-${dagName.stripSuffix(".yaml")}-dfg$i${config.outExt}"
                  val outFile = new File(resultSubDir, outFileName)

                  // Write the fullSource to the file
                  val writer = new FileWriter(outFile)
                  writer.write(combinedSourceWithResults)
                  writer.close()

                  if (stats("generated") % config.updateLiveStatsAfter == 0) {
                    val liveStatsWriter = new FileWriter(new File(config.outDir, "live-stats.txt"))
                    liveStatsWriter.write(prettyPrintStats(stats))
                    liveStatsWriter.close()
                  }

                  stats("generated") += 1
                } catch {
                  case ex: Exception =>
                    println("==========")
                    println(s"DFG construction or codegen failed for $dagName, attempt #$i. Reason: $ex")
                    println(ex)
                    println(ex.getStackTrace.mkString("\t", "\n\t", ""))
                    println("==========")
                } finally {
                  stats("attempts") += 1
                }
              }
            }
          } catch {
            case ex: ImpossibleDFGException =>
              stats("attempts") += 1
              println(s"DFG construction or codegen failed for $dagName. Reason: ${ex.getMessage}")
            case ex: Exception =>
              stats("attempts") += 1
              println(s"Failed to parse DAG file: $dagName. Reason: ${ex.getMessage}")
          }
        }
      }
    }

    val elapsedAfterGeneration = (System.currentTimeMillis() - startTime) / 1000
    println(s"Terminated after $elapsedAfterGeneration seconds.")
    println(prettyPrintStats(stats))
  }

  private def constructCombinedFileContents(result: Throwable, optResult: Throwable, unOptResult: Throwable, fullSourceOpt: String, fullSourceUnOpt: String): String = {
    val optFileContents = constructFileContents(optResult, fullSourceOpt)
    val unOptFileContents = constructFileContents(unOptResult, fullSourceUnOpt)
    s"""
      |$optFileContents
      |
      |$unOptFileContents
      |
      |/* ========== ORACLE RESULT ===================
      |$result
      |${decideStackTrace(result)}
      |""".stripMargin
  }

  private def constructFileContents(result: Throwable, fullSourceOpt: String): String = {

    val stackTrace = decideStackTrace(result)

    val fullResult = s"$result\n$stackTrace"
    Harness.embedCode(fullSourceOpt, fullResult, Harness.resultMark)
  }

  private def decideStackTrace(result: Throwable): String = {
    result match {
      case _ : fuzzer.exceptions.Success => ""
      case _ => s"${result.getStackTrace.mkString("\n")}"
    }
  }

}

/*
Terminated after 63 seconds.
=== STATS ===
----- Details -----
generated = 200
ExtendedAnalysisException = 82
AnalysisException = 110
dag-batch = 1
Success = 8
attempts = 213
------ Summary -----
Exiting after DFGs generated == 200
Throughput DAG -> DFG: 200/213 (93.90%)
Throughput DAG -> Valid: 8/213 (3.76%)
Throughput DFG -> Valid: 8/200 (4.00%)
=============
 */