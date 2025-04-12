package udfissues

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

// Extension that disables all optimizer rules
class NoopOptimizerExtensions extends (SparkSessionExtensions => Unit) with Serializable {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule(_ => new Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = plan // no-op
    })
  }
}

object DisableOptimizerExample {

  def main(args: Array[String]): Unit = {
    val master = if (args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("Optimizer Correctness Issue")
//      .config("spark.sql.extensions", classOf[NoopOptimizerExtensions].getName)
      .master(master)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val ndUDF: UserDefinedFunction = udf((s: Int) => {
      val r = scala.util.Random.nextInt(100)
      CustomStruct(r, r)
    }).asNondeterministic()

    val df2 = spark.range(5).select(ndUDF($"id").withField("c", lit(7)))
    println(df2.queryExecution.analyzed)
//    df2.show()

//    df2.collect().foreach { row =>
//      assert(row.getStruct(0).getInt(0) == row.getStruct(0).getInt(1))
//    }
  }
}
/*
Project [if (isnull(UDF(cast(id#3L as int)))) null else named_struct(a, UDF(cast(id#3L as int)).a, b, UDF(cast(id#3L as int)).b, c, 7) AS update_fields(UDF(id), WithField(7))#5]
+- Range (0, 5, step=1, splits=Some(4))
Project [if (isnull(UDF(cast(id#3L as int)))) null else named_struct(a, UDF(cast(id#3L as int)).a, b, UDF(cast(id#3L as int)).b, c, 7) AS update_fields(UDF(id), WithField(7))#5]
+- Range (0, 5, step=1, splits=Some(4))
 */