package udfissues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, when}

object CrashOnCaseWhen {
  def main(args: Array[String]): Unit = {
    val master = if (args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("Lost PushDown")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val numbersDF = spark.read.parquet("numbers.parquet")

    var dfCaseWhen = numbersDF.filter($"number" =!= lit("0"))

    for( a <- 1 to 5) {
      dfCaseWhen = dfCaseWhen.withColumn("number", when($"number" === lit(a.toString), lit("r"+a.toString)).otherwise($"number"))
    }

    dfCaseWhen.explain(true)


  }
}
