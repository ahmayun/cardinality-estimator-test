package fuzzer.templates

object Harness {

  val insertionMark = "[[INSERT]]"
  val resultMark = "[[RESULT]]"

  val imports =
    """
      |import org.apache.spark.sql.SparkSession
      |import org.apache.spark.sql.functions._
      |import fuzzer.global.State.sparkOption
      |import sqlsmith.FuzzTests.withoutOptimized
      |import fuzzer.templates.ComplexObject
      |import fuzzer.exceptions._
      |""".stripMargin

  val preloadedUDFDefinition =
    """
      |    val preloadedUDF = udf((s: Any) => {
      |      val r = scala.util.Random.nextInt()
      |      ComplexObject(r,r)
      |    }).asNondeterministic()
      |""".stripMargin

  val sparkProgramOptimizationsOn: String =
    s"""
      |$imports
      |
      |object Optimized {
      |
      |  def main(args: Array[String]): Unit = {
      |    val spark = sparkOption.get
      |$preloadedUDFDefinition
      |
      |$insertionMark
      |
      |    fuzzer.global.State.optDF = Some(sink)
      |  }
      |}
      |
      |try {
      |   Optimized.main(Array())
      |} catch {
      | case e =>
      |    fuzzer.global.State.optRunException = Some(e)
      |}
      |
      |if (fuzzer.global.State.optRunException.isEmpty)
      |   fuzzer.global.State.optRunException = Some(new Success("Success"))
      |/*
      |$resultMark
      |*/
      |""".stripMargin

  val sparkProgramOptimizationsOff: String =
    s"""
      |$imports
      |
      |object UnOptimized {
      |
      |  def main(args: Array[String]): Unit = {
      |    val spark = sparkOption.get
      |
      |$preloadedUDFDefinition
      |
      |    val sparkOpt = spark.sessionState.optimizer
      |    val excludableRules = {
      |      val defaultRules = sparkOpt.defaultBatches.flatMap(_.rules.map(_.ruleName)).toSet
      |      val rules = defaultRules -- sparkOpt.nonExcludableRules.toSet
      |      rules
      |    }
      |    val excludedRules = excludableRules.mkString(",")
      |    withoutOptimized(excludedRules) {
      |$insertionMark
      |
      |    fuzzer.global.State.unOptDF = Some(sink)
      |    }
      |  }
      |}
      |
      |
      |try {
      |   UnOptimized.main(Array())
      |} catch {
      | case e =>
      |    fuzzer.global.State.unOptRunException = Some(e)
      |}
      |
      |if (fuzzer.global.State.unOptRunException.isEmpty)
      |   fuzzer.global.State.unOptRunException = Some(new Success("Success"))
      |
      |/*
      |$resultMark
      |*/
      |""".stripMargin

/*
    val sparkOpt = spark.sessionState.optimizer
    val excludableRules = {
      val defaultRules = sparkOpt.defaultBatches.flatMap(_.rules.map(_.ruleName)).toSet
      val rules = defaultRules -- sparkOpt.nonExcludableRules.toSet
      rules.foreach(println)
      rules
    }
    val excludedRules = excludableRules.mkString(",")
    withoutOptimized(excludedRules) {
      val df2 = spark.range(5).select(ndUDF($"id").withField("c", lit(7)))
      df2.show()
      df2.collect().foreach {
        row =>
          assert(row.getStruct(0).getInt(0) == row.getStruct(0).getInt(1))
      }
    }
 */
  def embedCode(template: String, source: String, marker: String, indent: String = ""): String = {
    val indentedSource = source.linesIterator.map(line => indent + line).mkString("\n")
    template.replace(marker, indentedSource)
  }
}
