package clustertests

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import udfs.Pairs._

object TestPairs {

  def main(args: Array[String]): Unit = {
    val master = if(args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("Test Query Pairs")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Register UDFs
    spark.udf.register("to_upper", udf(toUpper))
    spark.udf.register("calculate_age", udf(calculateAge))
    spark.udf.register("concat_names", udf(concatNames))
    spark.udf.register("is_weekend", udf(isWeekend))
    spark.udf.register("calculate_discount", udf(calculateDiscount))
    spark.udf.register("extract_domain", udf(extractDomain))
    spark.udf.register("calculate_tax", udf(calculateTax))
    spark.udf.register("days_between", udf(daysBetween))
    spark.udf.register("month_name", udf(monthName))
    spark.udf.register("string_reverse", udf(stringReverse))

    pairs.zipWithIndex.foreach {
      case ((a, b), i) =>
        println(s"======= QUERY $i =======")
        spark.sql(a).show(5)
        println("--------------")
        spark.sql(b).show(5)
    }
  }
}
