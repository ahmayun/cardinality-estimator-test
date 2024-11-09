//package misc
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan}
//import org.apache.spark.sql.execution.FileSourceScanExec
//import org.apache.spark.sql.functions.{col, expr}
//import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
//
//object QueryStatComputer {
//
//  def countOperators(plan: LogicalPlan): (Int, Int, Int) = {
//    var scanCount = 0
//    var joinCount = 0
//    var aggCount = 0
//
//    plan.foreach {
//      case _: Filter => scanCount += 1 // Assuming ScanOperation represents scans
//      case _: Join => joinCount += 1
//      case _: Aggregate => aggCount += 1
//      case _ => // Ignore other operators
//    }
//
//    (scanCount, joinCount, aggCount)
//  }
//
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//      .appName("QueryOptTester")
//      .config("spark.sql.cbo.enabled", "true")
//      .config("spark.sql.cbo.joinReorder.enabled", "true")
//      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
//      .config("spark.sql.statistics.histogram.enabled", "true")
//      .master("local[*]")
//      .enableHiveSupport()
//      .getOrCreate()
//
//    spark.sparkContext.setLogLevel("ERROR")
//
//    val schema = StructType(Array(
//      StructField("businessentityid", IntegerType, nullable = false),
//      StructField("firstname", StringType, nullable = true),
//      StructField("lastname", StringType, nullable = true)
//    ))
//
//    val numRows = 100000
//
//    val personDF = spark.range(numRows)
//      .select(
//        (col("id") % 1000000 + 1).as("businessentityid"),  // Random businessentityid between 1 and 5000
//        expr("concat('First', id % 500)").as("firstname"),  // Firstname as 'First' followed by a random number
//        expr("concat('Last', id % 500)").as("lastname")    // Lastname as 'Last' followed by a random number
//      )
//
//    personDF.createOrReplaceTempView("person")
//    spark.sql("select max(distinct businessentityid) as max_id from person").explain(true)
//    println(org.apache.spark.sql.catalyst.rules.Rule.ruleApplicationCounts.mkString("\n"))
//  }
//
//}
