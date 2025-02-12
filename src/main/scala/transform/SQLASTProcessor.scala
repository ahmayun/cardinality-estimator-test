import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.ParserInterface

object SqlAstTransformer {

  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("SqlAstTransformer")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    val sqlQuery = """
                     | WITH customer_total_return AS
                     |   (SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk,
                     |           sum(sr_return_amt) AS ctr_total_return
                     |    FROM tpcds.store_returns, tpcds.date_dim
                     |    WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
                     |    GROUP BY sr_customer_sk, sr_store_sk)
                     | SELECT c_customer_id
                     |   FROM customer_total_return ctr1, tpcds.store, tpcds.customer
                     |   WHERE ctr1.ctr_total_return >
                     |    (SELECT avg(ctr_total_return) + 1.2
                     |      FROM customer_total_return ctr2
                     |       WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
                     |   AND s_store_sk = ctr1.ctr_store_sk
                     |   AND s_state = 'TN'
                     |   AND ctr1.ctr_customer_sk = c_customer_sk
                     |   ORDER BY c_customer_id LIMIT 100
                     |""".stripMargin

    val transformations: List[LogicalPlan => LogicalPlan] = List(
      transformAdditionToSubtraction
    )

    val logicalPlan = spark.sql(sqlQuery).queryExecution.analyzed
    val transformedQuery = transformSqlQuery(logicalPlan, spark.sessionState.sqlParser, transformations)

    println(s"Original Query:\n$sqlQuery")
    println(s"\nTransformed Query:\n$transformedQuery")

    spark.stop()
  }

  def transformSqlQuery(logicalPlan: LogicalPlan, parser: ParserInterface, transformations: List[LogicalPlan => LogicalPlan]): String = {
    try {
      val transformedPlan = transformations.foldLeft(logicalPlan) { (plan, transform) => transform(plan) }

      // Convert back to SQL
      transformedPlan.toString()
    } catch {
      case e: ParseException =>
        println(s"Failed to parse query: ${e.getMessage}")
        logicalPlan.toString()
    }
  }

  /**
   * Transformation function: Replace all `+` with `-` inside expressions, even within subqueries.
   */
  def transformAdditionToSubtraction(plan: LogicalPlan): LogicalPlan = {
//    println("============")
//    println(plan.toString())
    plan.transformDown {
      case subquery: SubqueryAlias =>
//        println("Subquery")
        subquery.copy(child = transformAdditionToSubtraction(subquery.child)) // Recursively transform subqueries

      case project: Project =>
//        println("project")
        project.copy(projectList = project.projectList.map(a => transformExpression(a).asInstanceOf[NamedExpression]))

      case filter: Filter =>
//        println("filter")
        filter.copy(condition = transformExpression(filter.condition))

      case aggregate: Aggregate =>
//        println("agg")
        aggregate.copy(aggregateExpressions = aggregate.aggregateExpressions.map(a => transformExpression(a).asInstanceOf[NamedExpression]))

      case other =>
//        println(s"other: ${other.toString()}")
//        other.mapChildren { p =>
//          println(p)
//          p
//        }
        other
    }
  }

  /**
   * Transform expressions inside a LogicalPlan.
   * Replaces `+` with `-` in any part of the SQL query.
   */
  def transformExpression(expr: Expression): Expression = {
//    println(s"expression: ${expr.sql}")
    expr.transform {
      case add: Add =>
        println("HIT")
        Subtract(add.left, add.right) // Replace `+` with `-`
    }
  }
}
