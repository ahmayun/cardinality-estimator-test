package misc

import org.apache.spark.sql.SparkSession
import scala.io.Source

object QueryRunner {

  def readFileLines(filePath: String): List[String] = {
    val source = Source.fromFile(filePath)
    try {
      source.getLines().toList
    } finally {
      source.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("QueryOptTester")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master("local[*]")
      //      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    var syntax_err = 0
    var analysis_err = 0
    var success = 0
    var other = 0

    val lines = readFileLines("/home/ahmad/Documents/proposal/sqlancer/target/logs/sqlite3/database0-cur.log")
    lines.take(40).foreach {
      q =>
        try {
          spark.sql(q).explain(true)
          success += 1
        } catch {
          case _: org.apache.spark.sql.catalyst.parser.ParseException =>
            syntax_err += 1
            print("ERR_SYN: ")
          case _: org.apache.spark.sql.AnalysisException =>
            print("ERR_ANL: ")
            analysis_err += 1
          case e: Throwable =>
            println(s"$e")
            print("ERR_OTH: ")
            other += 1
        }
        println(s"$q")
    }

    println(s"Successes: ${success}")
    println(s"Parse Errors: ${syntax_err}")
    println(s"Analysis Errors: ${analysis_err}")
    println(s"Other Issues: ${other}")
  }
}
