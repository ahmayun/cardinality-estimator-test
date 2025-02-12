package transform

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Add, Divide, Expression, Multiply, ScalarSubquery, Subtract}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import java.io.{File, PrintWriter}
import scala.io.Source

object QueryGenerator {

  val udfTemplate: String =
    """
      |val {UDFNAME}: UserDefinedFunction = udf({UDFBODY})
      |spark.udf.register("{UDFNAME}", {UDFNAME})
      |""".stripMargin

  val template: String =
    """package tpcds.runnables.{PACKAGE}

      |import org.apache.spark.sql.SparkSession
      |import org.apache.spark.sql.catalyst.rules.RuleExecutor
      |import org.apache.spark.sql.expressions.UserDefinedFunction
      |import org.apache.spark.sql.functions.udf
      |
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

  private def fixTableReferences(query: String): String = {
    val tableNames = List("call_center","catalog_page","catalog_returns","catalog_sales","customer","customer_address","customer_demographics","date_dim","household_demographics","income_band","inventory","item","promotion","reason","ship_mode","store","store_returns","store_sales","time_dim","warehouse","web_page","web_returns","web_sales","web_site")

    tableNames.foldLeft(query) { (currentQuery, tableName) =>
      val pattern = s"\\b$tableName\\b".r
      pattern.replaceAllIn(currentQuery, s"tpcds.$tableName")
    }
  }

  def processQuery(query: String): String = {
    val tripleQuotes = """""""*3
    val margin = "\t|"
    val indentedQuery = fixTableReferences(query).split("\n")
      .map(line => margin + line)
      .mkString("\n")
    s"$tripleQuotes\n$indentedQuery\n$tripleQuotes.stripMargin"
  }

  private def insertUDF(spark: SparkSession, queryContent: String, template: String): String = {
    val lp = spark.sql(queryContent).queryExecution.logical

//    def traverseAndTransformPlan(plan: LogicalPlan): LogicalPlan = {
//      // Transform expressions inside the plan
//      val newExpressions: Seq[Expression] = plan.expressions.map(transformExpression)
//
//      // Check for subqueries
//      newExpressions.foreach { expr =>
//        expr.foreach {
//          case subquery: ScalarSubquery =>
//            println(s"Found Scalar Subquery: ${subquery.plan}")
//          case _ => // Ignore other expressions
//        }
//      }
//
//      // Recursively transform children plans
//      val newChildren = plan.children.map(traverseAndTransformPlan)
//
//      // Return a new LogicalPlan with transformed expressions and children
//      plan.withNewChildren(newChildren).withNewExprs(newExpressions)
//    }
//
//    def transformExpression(expr: Expression): Expression = {
//      expr.transform {
//        case Add(left, right, _) =>
//          println(s"Transforming Add($left, $right) → Subtract($left, $right)")
//          Subtract(left, right)
//      }
//    }
//
//    traverseAndTransformPlan(lp)
    template
  }

  def generateScalaFiles(spark: SparkSession): Unit = {
    val queryDir = new File("src/main/scala/tpcds/queries/")
    val outputDirOriginal = new File("src/main/scala/tpcds/runnables/generated/original")
    val outputDirUDF = new File("src/main/scala/tpcds/runnables/generated/udf")

    if (!outputDirOriginal.exists()) outputDirOriginal.mkdirs()
    if (!outputDirUDF.exists()) outputDirUDF.mkdirs()

    queryDir.listFiles().filter(_.getName.matches("tpcds-q\\d+[a-z]*\\.sql")).foreach { file =>
//      val queryNumber = file.getName.replaceAll("[^0-9]", "")
      val queryPattern = "([0-9]+)([a-z]?)".r
      val queryNumber = queryPattern.findFirstIn(file.getName).getOrElse("")
      val queryContent = Source.fromFile(file).mkString
      val processedQuery = processQuery(queryContent)

      val scalaCode = template
        .replace("{N}", queryNumber)
        .replace("{QUERY}", processedQuery)

      val outputFile = new File(outputDirOriginal, s"Q$queryNumber.scala")
      val writer = new PrintWriter(outputFile)
      writer.write(scalaCode)
      writer.close()
      println(s"Generated Original: ${outputFile.getPath}")

      val udfInsertedCode = insertUDF(spark, queryContent, template)
      val outputFileUDF = new File(outputDirUDF, s"Q$queryNumber.scala")
      val writerUDF = new PrintWriter(outputFileUDF)
      writer.write(udfInsertedCode)
      writer.close()
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
