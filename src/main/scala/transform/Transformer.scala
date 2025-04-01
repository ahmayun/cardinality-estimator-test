package transform

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Sum}
import org.apache.spark.sql.catalyst.expressions.{Add, And, Divide, EqualTo, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, Literal, Multiply, ScalaUDF, Subtract}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType}
import pipelines.local.ComparePlans.MutationSummary

object Transformer {

  def fixTableReferences(query: String): String = {
    val tableNames = List("call_center","catalog_page","catalog_returns","catalog_sales","customer","customer_address","customer_demographics","date_dim","household_demographics","income_band","inventory","item","promotion","reason","ship_mode","store","store_returns","store_sales","time_dim","warehouse","web_page","web_returns","web_sales","web_site")

    tableNames.foldLeft(query) { (currentQuery, tableName) =>
      val pattern = s"\\b$tableName\\b".r
      pattern.replaceAllIn(currentQuery, s"tpcds.$tableName")
    }
  }

  def insertAndRegisterUDFs(spark: SparkSession, queryID: String, query: String): (LogicalPlan, MutationSummary) = {
    val lp = spark.sql(query).queryExecution.logical
    val summary = new MutationSummary()

    var udfID = 0
    val newLp: LogicalPlan = lp.transformAllExpressionsWithSubqueries {
      case x @ Multiply(l, r, _) =>
        val udf = ScalaUDF((a: Double, b: Double) => a*b, DataType.fromDDL("double"), Seq(l, r), udfName=Some(s"multiply$udfID"))
        val s = s"Transformed: $x => $udf"
        summary.logQueryInfo(queryID, s)
        println(s)
        udfID+=1
        udf
      case x @ Add(l, r, _) =>
        val udf = ScalaUDF((a: Double, b: Double) => a+b, DataType.fromDDL("double"), Seq(l, r), udfName=Some(s"add$udfID"))
        val s = s"Transformed: $x => $udf"
        summary.logQueryInfo(queryID, s)
        println(s)
        udfID+=1
        udf
      case x @ Subtract(l, r, _) =>
        val udf = ScalaUDF((a: Double, b: Double) => a-b, DataType.fromDDL("double"), Seq(l, r), udfName=Some(s"subtract$udfID"))
        val s = s"Transformed: $x => $udf"
        summary.logQueryInfo(queryID, s)
        println(s)
        udfID+=1
        udf
      case x @ Divide(l, r, _) =>
        val udf = ScalaUDF((a: Double, b: Double) => a/b, DataType.fromDDL("double"), Seq(l, r), udfName=Some(s"divide$udfID"))
        val s = s"Transformed: $x => $udf"
        summary.logQueryInfo(queryID, s)
        println(s)
        udfID+=1
        udf
      case x @ And(GreaterThanOrEqual(child, lower), LessThanOrEqual(child2, upper)) if child2 == child =>
        val udf = ScalaUDF(
          (v: Double, lb: Double, ub: Double) => v >= lb && v <= ub,
          DataType.fromDDL("boolean"),
          Seq(child, lower, upper),
          udfName = Some(s"between$udfID")
        )
        val s = s"Transformed: $x => $udf"
        summary.logQueryInfo(queryID, s)
        println(s)
        udfID += 1
        udf
      case x @ GreaterThan(l, r) =>
        val udf = ScalaUDF(
          (a: Double, b: Double) => a > b,
          DataType.fromDDL("boolean"),
          Seq(l, r),
          udfName = Some(s"greaterThan$udfID")
        )
        val s = s"Transformed: $x => $udf"
        summary.logQueryInfo(queryID, s)
        println(s)
        udfID += 1
        udf
      case x @ LessThan(l, r) =>
        val udf = ScalaUDF(
          (a: Double, b: Double) => a < b,
          DataType.fromDDL("boolean"),
          Seq(l, r),
          udfName = Some(s"lessThan$udfID")
        )
        val s = s"Transformed: $x => $udf"
        summary.logQueryInfo(queryID, s)
        println(s)
        udfID += 1
        udf

      case x @ GreaterThanOrEqual(l, r) =>
        val udf = ScalaUDF(
          (a: Double, b: Double) => a >= b,
          DataType.fromDDL("boolean"),
          Seq(l, r),
          udfName = Some(s"greaterThanOrEqual$udfID")
        )
        val s = s"Transformed: $x => $udf"
        summary.logQueryInfo(queryID, s)
        println(s)
        udfID += 1
        udf

      case x @ LessThanOrEqual(l, r) =>
        val udf = ScalaUDF(
          (a: Double, b: Double) => a <= b,
          DataType.fromDDL("boolean"),
          Seq(l, r),
          udfName = Some(s"lessThanOrEqual$udfID")
        )
        val s = s"Transformed: $x => $udf"
        summary.logQueryInfo(queryID, s)
        println(s)
        udfID += 1
        udf
      case x @ In(value, list) if list.forall(_.isInstanceOf[Literal]) =>
        val values = list.map(_.asInstanceOf[Literal].value)
        val dataType = list.head.asInstanceOf[Literal].dataType // Get type from the first element

        val udf = ScalaUDF(
          (v: Any, arr: Seq[Any]) => arr.contains(v), // Generic contains check
          DataType.fromDDL("boolean"),
          Seq(value, Literal.create(values, ArrayType(dataType))),
          udfName = Some(s"inArray$udfID")
        )

        val s = s"Transformed: $x => $udf"
        summary.logQueryInfo(queryID, s)
        println(s)
        udfID += 1
        udf
      case x @ EqualTo(left, right) =>
        val udf = ScalaUDF(
          (a: Any, b: Any) => a == b,  // Generic equality check
          DataType.fromDDL("boolean"),
          Seq(left, right),
          udfName = Some(s"equalTo$udfID")
        )
        val s = s"Transformed: $x => $udf"
        summary.logQueryInfo(queryID, s)
        println(s)
        udfID += 1
        udf
      case e => e
    }


    val newOptPlan = spark.sessionState.executePlan(newLp).optimizedPlan
    (newOptPlan, summary)
  }
}
