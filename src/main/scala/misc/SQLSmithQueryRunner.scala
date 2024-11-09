//package misc
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
//import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
//
//import scala.io.Source
//import org.apache.spark.sql.catalyst.rules.Rule.ruleApplicationCounts
//
//object SQLSmithQueryRunner {
//
//  def readFileLines(filePath: String): List[String] = {
//    val source = Source.fromFile(filePath)
//    try {
//      source.getLines().toList
//    } finally {
//      source.close()
//    }
//  }
//
//
//  def setupDummyDB(session: SparkSession): Unit = {
//
//
//    // Step 1: Initialize Spark session
//    val spark = SparkSession.builder()
//      .appName("SQLite to SparkSQL")
//      .config("spark.master", "local")  // or connect to your cluster
//      .getOrCreate()
//
//    // Step 2: Create schema for 'users' table
//    val usersSchema = new StructType()
//      .add(StructField("c0", IntegerType, nullable = false))
//      .add(StructField("id", IntegerType, nullable = false))
//      .add(StructField("c1", StringType, nullable = false))
//      .add(StructField("name", StringType, nullable = false))
//      .add(StructField("c2", StringType, nullable = false))
//      .add(StructField("email", StringType, nullable = false))
//
//    // Step 3: Create schema for 'orders' table
//    val ordersSchema = new StructType()
//      .add(StructField("c0", IntegerType, nullable = false))
//      .add(StructField("id", IntegerType, nullable = false))
//      .add(StructField("c1", IntegerType, nullable = false))
//      .add(StructField("user_id", IntegerType, nullable = false))
//      .add(StructField("c2", DoubleType, nullable = false))
//      .add(StructField("amount", DoubleType, nullable = false))
//      .add(StructField("c3", StringType, nullable = false))
//      .add(StructField("date", StringType, nullable = false))
//
//    // Step 4: Create data for 'users' table
//    val usersData = Seq(
//      Row(1, 1, "Alice", "Alice", "alice@example.com", "alice@example.com"),
//      Row(2, 2, "Bob", "Bob", "bob@example.com", "bob@example.com")
//    )
//
//    // Step 5: Create data for 'orders' table
//    val ordersData = Seq(
//      Row(1,1, 1, 1, 50.75, 50.75, "2023-10-10", "2023-10-10"),
//      Row(2,2, 2, 2, 100.00, 100.00, "2023-10-11", "2023-10-11"),
//      Row(3,3, 1, 1, 25.00, 25.00, "2023-10-12", "2023-10-12")
//    )
//
//    // Step 6: Convert data to DataFrames
//    val usersRDD = spark.sparkContext.parallelize(usersData)
//    val usersDF = spark.createDataFrame(usersRDD, usersSchema)
//    val ordersDF = spark.createDataFrame(spark.sparkContext.parallelize(ordersData), ordersSchema)
//
//    // Step 7: Create temporary views for SQL access
//    usersDF.createOrReplaceTempView("users")
//    ordersDF.createOrReplaceTempView("orders")
//
//    spark.sql("CREATE DATABASE IF NOT EXISTS main")
//    // Step 2: Save the DataFrames as tables within the 'main' database
//    usersDF.write.mode("overwrite").saveAsTable("main.users")
//    ordersDF.write.mode("overwrite").saveAsTable("main.orders")
//
//    println("Tables created in SparkSQL!")
//
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
//    var syntax_err = 0
//    var analysis_err = 0
//    var success = 0
//    var other = 0
//
////    setupDummyDB(spark)
////
////    spark.sql("select * from main.users").show(4)
////    spark.sql("select * from main.orders").show(4)
////    sys.exit(0)
//
//    val queries = QueryParser.getQueries("/home/ahmad/Documents/project/sqlsmith/sqlite3_testdb_2KC.log")
//    var counter = 0
//    queries.foreach {
//      q =>
//        try {
//          spark.sql(q).explain(true)
//          success += 1
//        } catch {
//          case _: org.apache.spark.sql.catalyst.parser.ParseException =>
//            syntax_err += 1
////            print("ERR_SYN: ")
//          case e: org.apache.spark.sql.AnalysisException =>
//            println(e)
////            print("ERR_ANL: ")
//            analysis_err += 1
//          case e: Throwable =>
////            println(s"$e")
////            print("ERR_OTH: ")
//            other += 1
//        } finally {
//          if(counter % 2000 == 0) {
//            println(s"Checkpoint @ Query #$counter")
//            println(s"Successes: ${success}")
//            println(s"Parse Errors: ${syntax_err}")
//            println(s"Analysis Errors: ${analysis_err}")
//            println(s"Other Issues: ${other}")
//
//            println(ruleApplicationCounts.mkString("\n"))
//            println(ruleApplicationCounts.size)
//            println(ruleApplicationCounts)
//          }
//          counter+=1;
//        }
////        println(s"$q")
//
//    }
//
//    println("FINAL RESULTS")
//    println(s"Successes: ${success}")
//    println(s"Parse Errors: ${syntax_err}")
//    println(s"Analysis Errors: ${analysis_err}")
//    println(s"Other Issues: ${other}")
//
//    println(ruleApplicationCounts.mkString("\n"))
//    println(ruleApplicationCounts.size)
//    println(ruleApplicationCounts)
//
//  }
//}
