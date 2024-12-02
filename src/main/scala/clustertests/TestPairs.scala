package clustertests

import org.apache.spark.sql.SparkSession
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
    spark.udf.register("to_upper", toUpper)
    //    spark.udf.register("calculate_age", calculateAge)
    //    spark.udf.register("concat_names", concatNames)
    //    spark.udf.register("is_weekend", isWeekend)
    //    spark.udf.register("calculate_discount", calculateDiscount)
    //    spark.udf.register("extract_domain", extractDomain)
    //    spark.udf.register("calculate_tax", calculateTax)
    //    spark.udf.register("days_between", daysBetween)
    //    spark.udf.register("month_name", monthName)
    //    spark.udf.register("string_reverse", stringReverse)

    spark.sql("SELECT to_upper(c_first_name) AS first_name_upper FROM main.customer;").show(4)
    //    pairs.zipWithIndex.foreach {
    //      case ((a, b), i) =>
    //        println(s"======= QUERY $i =======")
    //        spark.sql(a).show(5)
    //        println("--------------")
    //        spark.sql(b).show(5)
    //    }
  }
}
