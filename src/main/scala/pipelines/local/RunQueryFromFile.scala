package pipelines.local

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.parser.{AbstractParser, AstBuilder, CatalystSqlParser, DataTypeAstBuilder, ParserInterface, SqlBaseParser}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.errors.QueryParsingErrors


object RunQueryFromFile  {
  def transformQuery(query: String): String = {
    // Parse the query into a LogicalPlan
    val plan = CatalystSqlParser.parseExpression(query)

    // Transform the plan
    val transformedPlan = transformPlan(plan)

    // Convert back to SQL string
    transformedPlan.sql
  }

  private def transformPlan(plan: Expression): Expression = {
    plan.transformDown {
      case expr: Expression => transformExpression(expr)
    }
  }

  private def transformExpression(expr: Expression): Expression = expr match {
    // Handle arithmetic operations
    case add @ Add(left, right, dt) if isArithmeticChain(add) =>
      println("Add found")
      createUdfCall(collectOperands(add))
    case sub @ Subtract(left, right, dt) if isArithmeticChain(sub) =>
      createUdfCall(collectOperands(sub))
    case mul @ Multiply(left, right, dt) if isArithmeticChain(mul) =>
      println("mult found")
      createUdfCall(collectOperands(mul))
    case div @ Divide(left, right, dt) if isArithmeticChain(div) =>
      createUdfCall(collectOperands(div))

    // Handle simple arithmetic
    case Add(left, right, dt) =>
      println("add found")
      Add(transformExpression(left), transformExpression(right), dt)
    case Subtract(left, right, dt) => Subtract(transformExpression(left), transformExpression(right), dt)
    case Multiply(left, right, dt) => Multiply(transformExpression(left), transformExpression(right), dt)
    case Divide(left, right, dt) => Divide(transformExpression(left), transformExpression(right), dt)

    // Keep other expressions unchanged but transform their children
    case other => other.mapChildren(transformExpression)
  }

  // Check if this is part of a chain of arithmetic operations that should be combined
  private def isArithmeticChain(expr: Expression): Boolean = {
//    val operands = collectOperands(expr)
//    // Only transform if we have more than 2 operands (indicating a chain)
//    // or if we have exactly one column reference (to avoid transforming simple arithmetic)
//    operands.size > 2 || operands.count(_.isInstanceOf[UnresolvedAttribute]) == 1
    true
  }

  private def collectOperands(expr: Expression): Seq[Expression] = expr match {
    case Add(left, right, _) =>
      println("Add found")
      collectOperandsHelper(left) ++ collectOperandsHelper(right)
    case Subtract(left, right, _) => collectOperandsHelper(left) ++ collectOperandsHelper(right)
    case Multiply(left, right, _) => collectOperandsHelper(left) ++ collectOperandsHelper(right)
    case Divide(left, right, _) => collectOperandsHelper(left) ++ collectOperandsHelper(right)
    case lit: Literal => Seq(lit)
    case attr: UnresolvedAttribute => Seq(attr)
    case other => Seq(other)
  }

  private def collectOperandsHelper(expr: Expression): Seq[Expression] = expr match {
    case arithmetic: BinaryArithmetic => collectOperands(arithmetic)
    case other => Seq(other)
  }

  private def createUdfCall(operands: Seq[Expression]): Expression = {
    UnresolvedFunction(
      Seq("udf"),
      operands,
      isDistinct = false
    )
  }

  def main(args: Array[String]): Unit = {
    val queryID = 1
    val queryFilePath = s"src/main/scala/tpcds/queries/tpcds-q$queryID.sql"
    val master = if(args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("TPC-DS Q1")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

//    val query = dumpFile(queryFilePath)
    val query = "select col from customer"
//    val query =
//      """
//        |SELECT c_customer_id
//        |FROM tpcds.customer
//        |WHERE c_customer_sk IN (
//        |    SELECT sr_customer_sk
//        |    FROM tpcds.store_returns, tpcds.date_dim
//        |    WHERE sr_returned_date_sk = d_date_sk
//        |      AND d_year+1 = 2000
//        |)
//        |ORDER BY c_customer_id
//        |LIMIT 100;
//        |""".stripMargin

    println(transformQuery(query))

  }

}
