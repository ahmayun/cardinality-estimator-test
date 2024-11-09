package diff
import org.apache.spark.sql.{SparkSession, DataFrame}
import java.util.Properties

object CESparkSQL {
  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSession.builder()
      .appName("PostgresSparkApp")
      .master("local[*]")
      .getOrCreate()

    val jdbcUrl = "jdbc:postgresql://localhost:5432/Adventureworks"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "ahmad")
    connectionProperties.put("password", "1212")
    connectionProperties.put("driver", "org.postgresql.Driver")

    val query = """(select p.BusinessEntityID AS PersonBusinessEntityID,
      be.BusinessEntityID AS BusinessEntityBusinessEntityID,
      p.persontype
      from person.person p
      inner join person.BusinessEntity be
      on p.BusinessEntityID = be.BusinessEntityID
      where p.BusinessEntityID <= 120 AND p.persontype = 'EM')
      AS subquery"""

    println(s"Original Query:\n$query")

    try {
      // Load data from PostgreSQL
      val df: DataFrame = spark.read
        .jdbc(jdbcUrl, query, connectionProperties)

      // Create a temporary view for SQL querying in Spark
      val viewName = "view2"
      df.createOrReplaceTempView(viewName)

      // Execute Spark SQL queries
      println("Explaining query:")
      df.explain("cost")

      // Getting cardinality (count of rows)
      println("Getting cardinality:")
      val cardinality = spark.sql(s"SELECT COUNT(*) AS count FROM $viewName")
      cardinality.show()

      // Counting rows directly
      println("Counting:")
      val count = df.count()
      println(s"Count: $count")

      // Since Spark does not drop temporary views automatically, we can unpersist it
      spark.catalog.dropTempView(viewName)
      println(s"Dropped temporary view $viewName")

    } finally {
      // Stop the Spark session
      spark.stop()
    }
  }
}

