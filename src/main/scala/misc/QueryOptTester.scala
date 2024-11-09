package misc

import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object QueryOptTester {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("QueryOptTester")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = StructType(Array(
      StructField("businessentityid", IntegerType, nullable = false),
      StructField("firstname", StringType, nullable = true),
      StructField("lastname", StringType, nullable = true)
    ))

    import spark.implicits._

    // Create some sample data for the person table, including duplicate businessentityid values
    val numRows = 100000

    // Create a DataFrame with 10,000 rows, generating random businessentityid and random names
    val personDF = spark.range(numRows)
      .select(
        (col("id") % 1000000 + 1).as("businessentityid"),  // Random businessentityid between 1 and 5000
        expr("concat('First', id % 500)").as("firstname"),  // Firstname as 'First' followed by a random number
        expr("concat('Last', id % 500)").as("lastname")    // Lastname as 'Last' followed by a random number
      )


    // Create an RDD and then a DataFrame
//    val rdd = spark.sparkContext.parallelize(data)
//    val personDF = spark.createDataFrame(rdd, schema)

    // Register the DataFrame as a temporary view in the SQL context
    personDF.createOrReplaceTempView("person")
//    spark.sql("select max(distinct businessentityid) as max_id from person").explain(true)
    spark.sql("drop database tpch cascade").show()
//    spark.sql(s"use tpch")
//    spark.sql("select * from nation").show()
//    spark.sql(s"show tables").select("tableName").show()
//    println(org.apache.spark.sql.catalyst.rules.Rule.ruleApplicationCounts.mkString("\n"))
//    prettyPrintCoverage(org.apache.spark.sql.catalyst.rules.Rule.getOptimizationRuleCoverage)
//    spark.printOptimizerRuleCoverage()
  }


}
