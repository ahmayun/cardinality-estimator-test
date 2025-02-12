package udfissues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf


object LostPushDown {
  def main(args: Array[String]): Unit = {
    val master = if(args.isEmpty) "local[*]" else args(0)

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

    val udfPlusOneEq2: Int => Boolean = (k: Int) => {
      k+1==2
    }

    val udfPlusOne: Int => Int = (k: Int) => {
      k+1
    }

    spark.udf.register("plusOneEq2", udf(udfPlusOneEq2))
    spark.udf.register("plusOne", udf(udfPlusOne))

//    val numbers = (1 to 10000).map(i => new java.util.Random().nextInt(2)).toList.toDF("number")
//    numbers.write.parquet("numbers.parquet")
//    spark.catalog.listTables().show()

    val numbers = spark.read.parquet("numbers.parquet")
    numbers.createOrReplaceTempView("numbers")

    val q1 ="""
        |select number from numbers where number+1=2
        |""".stripMargin
    val q2 ="""
        |select number from numbers where plusOne(number)=2
        |""".stripMargin
    val q3 ="""
        |select number from numbers where plusOneEq2(number)
        |""".stripMargin

    val df1 = spark.sql(q1)
    val df2 = spark.sql(q2)
    val df3 = spark.sql(q3)



    numbers.show()
//    val df1 = numbers.filter(($"number" + 1) === 2)

    df1.explain("cost")
    println(s"Estimated count: ${df1.queryExecution.optimizedPlan.stats.rowCount}")
    println(s"Actual count: ${df1.count()}")
    println("=================================================")
    df2.explain("cost")
    println("=================================================")
    df3.explain("cost")
  }
}
