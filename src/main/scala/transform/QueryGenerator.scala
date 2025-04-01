package transform

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import transform.Transformer._

import java.io.{File, PrintWriter}
import scala.io.Source

object QueryGenerator {

  val template: String =
    """package tpcds.runnables.generated.{PACKAGE}

      |import org.apache.spark.sql.SparkSession
      |import org.apache.spark.sql.catalyst.rules.RuleExecutor
      |import org.apache.spark.sql.expressions.UserDefinedFunction
      |import org.apache.spark.sql.functions.udf
      |
      |/*
      |{COMMENT}
      |*/
      |object Q{N} {
      |
      |  def main(args: Array[String]): Unit = {
      |    val master = if(args.isEmpty) "local[*]" else args(0)
      |
      |    val spark = SparkSession.builder()
      |      .appName("TPC-DS Q{N}")
      |      .config("spark.sql.cbo.enabled", "true")
      |      .config("spark.sql.cbo.joinReorder.enabled", "true")
      |      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      |      .config("spark.sql.statistics.histogram.enabled", "true")
      |      .master(master)
      |      .enableHiveSupport()
      |      .getOrCreate()
      |
      |    spark.sparkContext.setLogLevel("ERROR")
      |
      |    val q = {QUERY}
      |
      |    spark.sql(q).explain(true)
      |
      |    val st = System.nanoTime()
      |    spark.sql(q).show(5)
      |    val et = System.nanoTime()
      |
      |    println(s"Total time (s): ${(et-st) / 1e9}")
      |    println(RuleExecutor.dumpTimeSpent().split("\n").filter(!_.contains("0 / ")).mkString("\n"))
      |  }
      |}
      |""".stripMargin

  def makeScalaString(query: String): String = {
    val tripleQuotes = """""""*3
    val margin = "\t|"
    val indentedQuery = query.split("\n")
      .map(line => margin + line)
      .mkString("\n")
    s"$tripleQuotes\n$indentedQuery\n$tripleQuotes.stripMargin"
  }

  def generateScalaFiles(spark: SparkSession): Unit = {
    val queryDir = new File("src/main/scala/tpcds/queries/")
    val outputDirOriginal = new File("src/main/scala/tpcds/runnables/generated/original")
    val outputDirUDF = new File("src/main/scala/tpcds/runnables/generated/udf")

    if (!outputDirOriginal.exists()) outputDirOriginal.mkdirs()
    if (!outputDirUDF.exists()) outputDirUDF.mkdirs()

    queryDir
      .listFiles()
      .filter(_.getName.matches("tpcds-q\\d+[a-z]*\\.sql"))
      .sorted
      .take(1)
      .foreach { file =>
  //      val queryNumber = file.getName.replaceAll("[^0-9]", "")
        val queryPattern = "([0-9]+)([a-z]?)".r
        val queryNumber = queryPattern.findFirstIn(file.getName).getOrElse("")
        val queryContent = Source.fromFile(file).mkString
        val processedQuery = fixTableReferences(queryContent)
        val queryScalaString = makeScalaString(processedQuery)

        val scalaCodeOrig = template
          .replace("{PACKAGE}", "original")
          .replace("{N}", queryNumber)
          .replace("{QUERY}", queryScalaString)
          .replace("{COMMENT}", "Original Query")


        val outputFile = new File(outputDirOriginal, s"Q$queryNumber.scala")
        val writer = new PrintWriter(outputFile)
        writer.write(scalaCodeOrig)
        writer.close()
        println(s"Generated Original: ${outputFile.getPath}")

        val (udfInsertedCode, summary) = insertAndRegisterUDFs(spark, queryNumber, processedQuery)
        val scalaCodeUDF = template
          .replace("{PACKAGE}", "udf")
          .replace("{N}", queryNumber)
          .replace("{QUERY}", makeScalaString(udfInsertedCode.toString()))
          .replace("{COMMENT}", summary.toString)

        val outputFileUDF = new File(outputDirUDF, s"Q$queryNumber.scala")
        val writerUDF = new PrintWriter(outputFileUDF)
        writerUDF.write(scalaCodeUDF)
        writerUDF.close()
        println(s"Generated UDF code: ${outputFileUDF.getPath}")
    }
  }

  def main(args: Array[String]): Unit = {
    val master = if(args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("QueryGenerator")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()

    generateScalaFiles(spark)
  }
}
