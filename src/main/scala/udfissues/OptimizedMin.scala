package udfissues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import sqlsmith.FuzzTests.withoutOptimized


object OptimizedMin {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Fuzzer")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    val preloadedUDF = udf((s: Any) => {
      println(s"called with $s")
      val r = scala.util.Random.nextInt()
      r //ComplexObject(r,r)
    }).asNondeterministic()



    val auto5 = spark.read.parquet("tpcds-min/ship_mode").as("ship_mode")
    val auto6 = auto5.orderBy(preloadedUDF(col("ship_mode.sm_ship_mode_sk")), col("ship_mode.sm_ship_mode_id"))
    auto6.show()
  }
}


