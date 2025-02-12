package udfissues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}


// This example no longer works as of spark 3.3
// Spark 3.3 is smart enough not to collapse projections when expensive expressions will need to be reevaluated as a result.

case class StructuredInformation(a: Integer, plusOne: Integer, squared: Integer)

object OptimizerSlowsDown {
  def main(args: Array[String]): Unit = {
    val master = if(args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("Optimizer Slowdown")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    val as = spark.read.parquet("numbers.parquet")

    def f(a: Int): StructuredInformation = {
      //mimic a long-running program
      Thread.sleep(10000)
      StructuredInformation(a, a+1, a*a)
    }

    val fUdf = udf(f _)

//    val asStructuredInfo = as.withColumn("structured_info", fUdf(col("a")))

    val exploded = as
      .withColumn("structured_info", fUdf(col("number")))
      .withColumn("plus_one", col("structured_info")("plusOne"))
      .withColumn("squared", col("structured_info")("squared"))

    import spark.implicits._
    def randomChars(i: Int): String = {
      (1 to 10).map( _ => scala.util.Random.nextPrintableChar().toString).mkString(",")
    }
//    val l = (1 to 10000)
//      .map(i => randomChars(i)).toList.toDF("list")
//    l.write.parquet("list.parquet")

//    spark.catalog.listTables().show()

    val df = spark.read.parquet("list.parquet")
    df.show()
    df.createOrReplaceTempView("src_split")
    spark.sql("select array[0] as a, array[1] as b, array[2] as c from (select split(list, ',') as array from src_split) t;").explain(true)

  }

}
