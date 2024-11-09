package diff

import org.apache.spark.sql.{SparkSession, DataFrame}
import scala.util.Random
import sys.process._

object CESparkBadJoinEstimate {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PostgresSparkApp")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val numRows = 1000
    val stringSet = Seq("apple", "banana", "cherry", "date", "elderberry")

    def getRandomString: String = {
      stringSet(Random.nextInt(stringSet.length))
    }

    val data1 = (0 until numRows by 2).map(i => (i, getRandomString))
    val df1 = spark.createDataFrame(data1).toDF("join_key", "random_string")

    val data2 = (1 until numRows by 2).map(i => (i, getRandomString))
    val df2 = spark.createDataFrame(data2).toDF("join_key", "random_string")

    df1.show(5)
    df2.show(5)


    val result1 = "rm -rf spark-warehouse/t1".!!
    val result2 = "rm -rf spark-warehouse/t2".!!
    println(result1, result2)

    df1.write.mode("overwrite").saveAsTable("t1")
    df2.write.mode("overwrite").saveAsTable("t2")

    spark.sql("ANALYZE TABLE t1 COMPUTE STATISTICS")
    spark.sql("ANALYZE TABLE t2 COMPUTE STATISTICS")

    val t1 = spark.table("t1")
    val t2 = spark.table("t2")

    val df_joined = t1.join(t2, usingColumn = "join_key")

    df_joined.explain("cost")

    println(s"Actual count: ${df_joined.count()}")
    df_joined.show(5)

  }

}
