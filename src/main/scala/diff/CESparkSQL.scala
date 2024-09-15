package diff

import org.apache.spark.sql.SparkSession

object CESparkSQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SparkSQL CE Example")
      .getOrCreate()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/Adventureworks")
      .option("dbtable", "<schema>.<table>")
      .option("user", "ahmad")
      .option("password", "1212")
      .load()

    // Sample data creation
    import spark.implicits._

    val data = Seq(
      ("Alice", 29),
      ("Bob", 31),
      ("Charlie", 35)
    )

    val df = data.toDF("name", "age")

    // Register DataFrame as a temporary view
    df.createOrReplaceTempView("people")

    // Write the SQL query
    val query = "SELECT name FROM people WHERE age > 30"

    // Get the DataFrame for the query
    val queryDF = spark.sql(query)

    // Explain the plan, which includes cardinality estimation
    queryDF.explain(true) // Use "true" to get a detailed physical plan with stats

    // Retrieve the logical plan and its stats
    val logicalPlan = queryDF.queryExecution.optimizedPlan
    val stats = logicalPlan.stats

    println(s"Estimated number of rows: ${stats.rowCount.getOrElse("No estimate available")}")

    // Stop Spark Session
    spark.stop()
  }

}
