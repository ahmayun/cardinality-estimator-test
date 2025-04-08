package fuzzer.core
import fuzzer.data.tables.{ColumnMetadata, TableMetadata}
import fuzzer.data.types.IntegerType
import fuzzer.exceptions.ImpossibleDFGException
import fuzzer.generation.Graph2Code.{constructDFG, dag2Scala}
import fuzzer.graph.{DAGParser, DFOperator, Graph}
import play.api.libs.json.JsValue
import utils.json.JsonReader

import scala.sys.process._
import utils.Random

import java.io.{File, FileWriter}

object OracleSystem {
  def check(source: String): Seq[String] = {
    // Stub: check the generated source, return a list of errors (if any)
    Seq.empty
  }
}


object MainFuzzer {
  def deleteDir(path: String): Unit = {
    val cmd = s"rm -rf ${path}"
    val exitCode = cmd.!
    if (exitCode != 0) {
      println(s"Failed to delete $path")
    }
  }

  def main(args: Array[String]): Unit = {
    // Hard-coded table metadata for demonstration
    Random.setSeed(fuzzer.global.Config.seed)

    val hardcodedTables: List[TableMetadata] = List(
      TableMetadata(
        _identifier = "users",
        _columns = Seq(
          ColumnMetadata("id", IntegerType, isNullable = false, isKey = true),
        ),
        _metadata = Map("source" -> "auth_system")
      ),
      TableMetadata(
        _identifier = "orders",
        _columns = Seq(
          ColumnMetadata("id", IntegerType, isNullable = false, isKey = true),
        ),
        _metadata = Map("source" -> "ecommerce")
      ),
      TableMetadata(
        _identifier = "products",
        _columns = Seq(
          ColumnMetadata("id", IntegerType, isNullable = false, isKey = true),
        ),
        _metadata = Map("source" -> "inventory")
      )
    )

    // -----------------------------
    // Configuration Parameters
    // -----------------------------
    val d = 100            // Number of DAG YAML files to generate per iteration
    val p = 1            // Number of DFGs to fill per DAG
    val outDir = "./out" // Output directory for generated programs
    val outExt = ".scala" // Extension of the output files
    val timeLimitSec = 5
    val generateCmd = "./dag-gen/venv/bin/python dag-gen/run_generator.py -c dag-gen/sample_config/dfg-config.yaml"
    val dagGenDir = "dag-gen/DAGs/DAGs"
    deleteDir(dagGenDir)
    deleteDir(outDir)

    var successful = 0
    var total = 0


    val specScala = JsonReader.readJsonFile("specs/spark-scala.json")

    new File(outDir).mkdirs()

    val startTime = System.currentTimeMillis()

    // Main fuzzer loop
    while (true) {
      val elapsed = (System.currentTimeMillis() - startTime) / 1000
      if (successful == 1 || elapsed >= timeLimitSec) {
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

        println(s"YAML FILES: ${yamlFiles.mkString(", ")}")

        yamlFiles.foreach { yamlFile =>
          val dagName = yamlFile.getName

          try {
            val dag = DAGParser.parseYamlFile(yamlFile.getAbsolutePath, map => DFOperator.fromMap(map))
            total += 1
            // Trial before looping, will throw ImpossibleDFGException if graph has a node with in-degree > 2
//            constructDFG(dag, specScala, hardcodedTables)
            total -= 1

            for (i <- 1 to p) {
              total += 1
              try {
                val dfg = constructDFG(dag, specScala, hardcodedTables)
                val generatedSource = dfg.generateCode(dag2Scala(specScala))
                val outFileName = s"${dagName.stripSuffix(".yaml")}-dfg$i$outExt"
                val outFile = new File(outDir, outFileName)
                val writer = new FileWriter(outFile)
                writer.write(generatedSource.toString)
                writer.close()

                val errors = OracleSystem.check(generatedSource.toString)
                if (errors.nonEmpty) {
                  println(s"Errors reported by Oracle for $outFileName: $errors")
                }
                successful += 1
                if (successful == 1) {
                  println(s"Exiting after successful == $successful")
                  val perSuccess = (successful.toFloat/total.toFloat)*100
                  println(f"$successful/$total ($perSuccess%.2f%%)")
                  sys.exit(0)
                }
              } catch {
                case ex: Exception =>
                  println(s"DFG construction or codegen failed for $dagName, attempt #$i. Reason: ${ex.getMessage}")
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
