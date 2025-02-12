package udfissues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

import scala.util.Random


object LostWSCG {
  def main(args: Array[String]): Unit = {
    val master = if(args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("Lost PushDown")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .config("spark.sql.codegen.wholeStage", "true")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    case class CustomType(value: Int)
    val udfNonDet: Int => Int = (k: Int) => {
      val obj = CustomType(k)
      obj.value+Random.nextInt(10)
    }



    spark.udf.register("notDetMult", udf(udfNonDet))

//    val numbers = (1 to 10000).map(i => new java.util.Random().nextInt(2)).toList.toDF("number")
//    numbers.write.parquet("numbers.parquet")
//    spark.catalog.listTables().show()

    val numbers = spark.read.parquet("numbers.parquet")
    numbers.createOrReplaceTempView("numbers")

    val q1 ="""
        |select number+1 from numbers where number>0.5
        |""".stripMargin
    val q2 ="""
        |select notDetMult(number) from numbers where number>0.5
        |""".stripMargin

    val df1 = spark.sql(q1)
    val df2 = spark.sql(q2)



    df1.explain(true)
    println(s"Estimated count: ${df1.queryExecution.optimizedPlan.stats.rowCount}")
    println(s"Actual count: ${df1.count()}")
    println("=================================================")
    df2.explain(true)
//    println("=================================================")
//    df1.queryExecution.debug.codegen
//    println("=================================================")
//    df2.queryExecution.debug.codegen
  }
}
