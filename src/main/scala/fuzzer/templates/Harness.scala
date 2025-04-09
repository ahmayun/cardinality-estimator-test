package fuzzer.templates

object Harness {

  val insertionMark = "[[INSERT]]"

  val sparkProgramOptimizationsOn =
    s"""
      |import org.apache.spark.sql.SparkSession
      |import org.apache.spark.sql.functions._
      |import fuzzer.global.State.spark
      |
      |object FuzzerGeneratedProgramHarness {
      |
      |  def main(args: Array[String]): Unit = {
      |
      |$insertionMark
      |
      |  }
      |}
      |
      |FuzzerGeneratedProgramHarness.main(Array())
      |""".stripMargin

  val sparkProgramOptimizationsOff =
    """
      |
      |""".stripMargin


  def embedCode(template: String, source: String, marker: String): String = {
    val indent = "    " // 4 spaces
    val indentedSource = source.linesIterator.map(line => indent + line).mkString("\n")
    template.replace(marker, indentedSource)
  }
}
