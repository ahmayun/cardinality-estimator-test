package clustertests

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TestTableRename {
  def main(args: Array[String]): Unit = {
    val master = if(args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("TestTableRename")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val a = spark.range(20, 30).as("a")
    val b = spark.range(20, 30).as("b")
    val joined = a
      .join(b, col("a.id") === col("b.id"))
      .withColumn("cid", col("a.id")*(-1))
    joined.show(5)

  }
}
