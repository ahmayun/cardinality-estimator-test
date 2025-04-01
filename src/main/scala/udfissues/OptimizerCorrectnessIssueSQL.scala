package udfissues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, udf}

case class CustomStructB(a: Int, b: Int)

object OptimizerCorrectnessIssueSQL {
  def main(args: Array[String]): Unit = {
    val master = if(args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("Optimizer Correctness Issue")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val randomPairFunc: Int => CustomStructB = (s: Int) => {
      val r = scala.util.Random.nextInt()
      CustomStructB(r, r)
    }

    val ndUDF = udf(randomPairFunc).asNondeterministic()

    spark.udf.register("randomPairUDF", ndUDF)

//    spark.sql(
//      """
//        |CREATE OR REPLACE FUNCTION randomPairUDF()
//        |RETURNS STRUCT<field1: INT, field2: INT>
//        |RETURN NAMED_STRUCT('field1', RAND() * 2147483647, 'field2', RAND() * 2147483647);
//        |""".stripMargin)
    val numbers = spark.read.parquet("numbers.parquet")
    numbers.createOrReplaceTempView("numbers")

    val df1 = spark.sql(
      """
        |SELECT randomPairUDF(number) AS result
        |FROM numbers;
        |""".stripMargin)

    val df2 = spark.sql(
      """
        |SELECT
        |  WithField(randomPairUDF(number), c, 7) AS result
        |FROM numbers;
        |""".stripMargin)

//    val df1 = spark.range(5).select(ndUDF($"id"))
//    val df2 = spark.range(5).select(ndUDF($"id").withField("c", lit(7)))


    println("==================")
    df1.collect().foreach(println)
    println("--------------------")
    df2.collect().foreach(println)

    println("==================")

    df1.explain(true)
    println("--------------------")
    df2.explain(true)

    println("==================")

    df1.collect().foreach {
      row =>
        assert(row.getStruct(0).getInt(0) == row.getStruct(0).getInt(1))
    }
    println("------------------")
    df2.collect().foreach {
      row =>
        assert(row.getStruct(0).getInt(0) == row.getStruct(0).getInt(1))
    }

  }

}
