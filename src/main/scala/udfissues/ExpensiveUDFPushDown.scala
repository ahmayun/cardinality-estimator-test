package udfissues

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ExpensiveUDFPushDown {

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

    val expensiveUdf = spark.udf.register("expensive_udf", (i: Int) => Option(i))
//    spark.range(10).write.format("orc").save("/tmp/orc")

//    val df = spark.read.format("orc").load("/tmp/orc").as("a")
//      .join(spark.range(10).as("b"), "id")
//      .withColumn("udf_op", expensiveUdf($"a.id"))
//      .join(spark.range(10).as("c"), $"udf_op" === $"c.id")
//
//    df.show()


/*
inter6 = spark.range(10).as("a")()
inter5 = spark.range(10).as("b")()
inter4 = inter5.join([in2],"id")(inter6)
inter3 = inter4.withColumn("udf_op", [gen_udf])()
inter2 = spark.range(10).as("c")()
inter1 = inter2.join([in2], $"udf_op" === $"c.id")(inter3)
inter0 = inter1.show()
 */
//    val inter8 = spark.range(10)
//    val inter7 = spark.read.format("orc").load("/tmp/orc")
//    val inter6 = inter8.as("b")
//    val inter5 = inter7.as("a")
//    val inter4 = spark.range(10)
//    val inter3 = inter5.join(inter6, "id")
//    val inter2 = inter4.as("c")
//    val inter1 = inter3.withColumn("udf_op", expensiveUdf($"a.id"))
//    val inter0 = inter1.join(inter2, $"udf_op" === $"c.id")
//    inter0.show()

    val A = spark.range(10)
    val B = spark.range(10)
    val C = spark.range(10)

    val auto0 = spark.range(85, 87).withColumnRenamed("id", "uid").as("users")
    val auto6 = spark.range(97).withColumnRenamed("id", "oid").as("orders")
    val auto4 = auto0.withColumn("o5WFP", col("users.uid") > 5)
    val auto5 = auto4.as("Pa2nY")
    val auto7 = auto6.join(auto5, col("Pa2nY.uid") === col("orders.oid"), "right")
    val auto8 = auto7.withColumn("NNRcV", col("orders.oid") > 5)
    val auto1 = auto8.join(auto0, col("Pa2nY.uid") === col("orders.oid"), "right")
    val auto3 = auto1.as("jBYrW")
    val auto9 = auto6.join(auto3, col("jBYrW.uid") === col("jBYrW.oid"), "inner")
    val auto10 = auto9.as("bEZIc")
    auto10.explain(true)


  }
  /*
  +---+------+---+
| id|udf_op| id|
+---+------+---+
|  7|     7|  7|
|  8|     8|  8|
|  9|     9|  9|
|  2|     2|  2|
|  3|     3|  3|
|  4|     4|  4|
|  5|     5|  5|
|  6|     6|  6|
|  0|     0|  0|
|  1|     1|  1|
+---+------+---+
   */

}
