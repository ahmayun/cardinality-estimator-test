package fuzzer.core
import fuzzer.data.tables.Examples.tpcdsTables
import fuzzer.data.tables.{ColumnMetadata, TableMetadata}
import fuzzer.oracle.OracleSystem
import fuzzer.exceptions.ImpossibleDFGException
import fuzzer.generation.Graph2Code.{constructDFG, dag2Scala}
import fuzzer.graph.{DAGParser, DFOperator, Graph}
import fuzzer.templates.Harness
import org.apache.spark.sql.{AnalysisException, SparkSession}
import play.api.libs.json.JsValue
import utils.json.JsonReader

import scala.sys.process._
import utils.Random

import java.io.{File, FileWriter}


object MainFuzzer {
  def deleteDir(path: String): Unit = {
    val cmd = s"rm -rf ${path}"
    val exitCode = cmd.!
    if (exitCode != 0) {
      println(s"Failed to delete $path")
    }
  }

  private def isInvalidDFG(dag: Graph[DFOperator], specScala: JsValue, hardcodedTables: List[TableMetadata]): Boolean = {
    dag.nodes.exists(_.getInDegree > 2)
  }

  def main(args: Array[String]): Unit = {
    // Hard-coded table metadata for demonstration
    Random.setSeed(fuzzer.global.Config.seed)

    // -----------------------------
    // Configuration Parameters
    // -----------------------------
    val exitAfterSingleSuccess = false
    val d = 200            // Number of DAG YAML files to generate per iteration
    val p = 10            // Number of DFGs to fill per DAG
    val outDir = "./out" // Output directory for generated programs
    val outExt = ".scala" // Extension of the output files
    val timeLimitSec = 5
    val generateCmd = "./dag-gen/venv/bin/python dag-gen/run_generator.py -c dag-gen/sample_config/dfg-config.yaml"
    val dagGenDir = "dag-gen/DAGs/DAGs"
    deleteDir(dagGenDir)
    deleteDir(outDir)

    var successful = 0
    var total = 0

    val master = if(args.isEmpty) "local[*]" else args(0)

    val sparkSession = SparkSession.builder()
      .appName("Fuzzer")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    fuzzer.global.State.spark = sparkSession

    val specScala = JsonReader.readJsonFile("specs/spark-scala-excl_show.json")

    new File(outDir).mkdirs()

    val startTime = System.currentTimeMillis()

    // Main fuzzer loop
    while (true) {
      val elapsed = (System.currentTimeMillis() - startTime) / 1000
      if (elapsed >= timeLimitSec) {
        println(s"Time limit of $timeLimitSec seconds reached. Terminating fuzzer.")
        sys.exit(0)
      }

      val exitCode = generateCmd.!
      if (exitCode != 0) {
        println(s"Warning: DAG generation command failed with exit code $exitCode")
        sys.exit(-1)
      }

      val dagFolder = new File(dagGenDir)
      if (!dagFolder.exists() || !dagFolder.isDirectory) {
        println("Warning: 'DAGs' folder not found or not a directory. Exiting.")
        sys.exit(-1)

      } else {

        val yamlFiles = dagFolder
          .listFiles()
          .filter(f => f.isFile && f.getName.startsWith("dag") && f.getName.endsWith(".yaml"))
          .take(d)

//        println(s"YAML FILES: ${yamlFiles.mkString(", ")}")

        yamlFiles.foreach { yamlFile =>
          val dagName = yamlFile.getName

          try {
            val dag = DAGParser.parseYamlFile(yamlFile.getAbsolutePath, map => DFOperator.fromMap(map))

            // Trial before looping, will throw ImpossibleDFGException if graph has a node with in-degree > 2
            if (isInvalidDFG(dag, specScala, tpcdsTables)) {
              total += 1
              throw new ImpossibleDFGException("Impossible to convert DAG to DFG. Has a node with in-degree > 2")
            }

            for (i <- 1 to p) {
              total += 1
              fuzzer.global.State.iteration += 1
              try {
                val selectedTables = Random.shuffle(tpcdsTables).take(dag.getSourceNodes.length).toList
                val dfg = constructDFG(dag, specScala, selectedTables)
                val generatedSource = dfg.generateCode(dag2Scala(specScala))

                val fullSource = Harness.embedCode(
                  Harness.sparkProgramOptimizationsOn,
                  generatedSource.toString,
                  Harness.insertionMark
                )

                val result = OracleSystem.check(fullSource)

                // Create subdirectory inside outDir using the result value
                val resultSubDir = new File(outDir, result.toString)
                resultSubDir.mkdirs() // Creates the directory if it doesn't exist

                // Prepare output file in the result-named subdirectory
                val outFileName = s"${dagName.stripSuffix(".yaml")}-dfg$i$outExt"
                val outFile = new File(resultSubDir, outFileName)

                // Write the fullSource to the file
                val writer = new FileWriter(outFile)
                writer.write(fullSource)
                writer.close()

                successful += 1
                if (exitAfterSingleSuccess && successful == 1) {
                  println(s"Exiting after successful == $successful")
                  val perSuccess = (successful.toFloat/total.toFloat)*100
                  println(f"$successful/$total ($perSuccess%.2f%%)")
                  sys.exit(0)
                }
              } catch {
                case ex: Exception =>
                  println(s"DFG construction or codegen failed for $dagName, attempt #$i. Reason: ")
                  println(ex)
                  println(ex.getStackTrace.mkString("\t", "\n\t", ""))
              }
            }
          } catch {
            case ex: ImpossibleDFGException =>
              println(s"DFG construction or codegen failed for $dagName. Reason: ${ex.getMessage}")
            case ex: Exception =>
              println(s"Failed to parse DAG file: $dagName. Reason: ${ex.getMessage}")
          }
        }

        val elapsedAfterGeneration = (System.currentTimeMillis() - startTime) / 1000
        if (elapsedAfterGeneration >= timeLimitSec) {
          println(s"Time limit of $timeLimitSec seconds reached after generation. Terminating.")
          val perSuccess = (successful.toFloat/total.toFloat)*100
          println(f"$successful/$total ($perSuccess%.2f%%)")
          sys.exit(0)
        }
      }

    }
  }


}
