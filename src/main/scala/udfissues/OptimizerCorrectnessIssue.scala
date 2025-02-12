package udfissues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.catalyst.optimizer.PushDownPredicates

case class CustomStruct(a: Int, b: Int)

object OptimizerCorrectnessIssue {
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

    val ndUDF = udf((s: Int) => {
      val r = scala.util.Random.nextInt()
      //both values should be the same
      CustomStruct(r,r)
    }).asNondeterministic()

    val df1 = spark.range(5).select(ndUDF($"id"))
    val df2 = spark.range(5).select(ndUDF($"id").withField("c", lit(7)))


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
