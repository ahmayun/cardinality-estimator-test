package diff

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import scala.sys.process._
import scala.util.Random

object CESparkBadFilterEstimate {

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

    val per_bucket = 10000
    val numRows = per_bucket*254 // 254 buckets, each will have 1000 values
    val stringSet = Seq("apple", "banana", "cherry", "date", "elderberry")

    def getRandomString: String = {
      stringSet(Random.nextInt(stringSet.length))
    }

    val data1 = (0 until numRows).map {
      i =>
        val ceil = (i/per_bucket + 1)*per_bucket
        val floor = i/per_bucket*per_bucket
        if((i+1) % per_bucket == 0) {
          (ceil, getRandomString)
        } else if(i%per_bucket == 0) {
          (i, getRandomString)
        } else {
//          (ceil-1, getRandomString)
          (floor, getRandomString)
        }
    }
    val df1 = spark.createDataFrame(data1).toDF("num", "random_string")

    df1.show(5)
    spark.createDataFrame(
      spark.sparkContext.parallelize(df1.tail(5)), df1.schema
    ).show(5)

    val result = "rm -rf spark-warehouse/t1f".!!
    println(result)

    df1.write.mode("overwrite").saveAsTable("t1f")

    spark.sql("ANALYZE TABLE t1f COMPUTE STATISTICS")


//    val filtered = df1.filter(col("num") > 253000)
    val filtered = df1.filter(col("random_string") > "connor")
    filtered.explain("cost")
    println(s"Actual count: ${filtered.count()}")
  }

}
