package diff

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import sys.process._

// Remember to delete spark-warehouse directory before running
object CEPureSparkSQL {

  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSession.builder()
      .appName("PostgresSparkApp")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    // Define the schema for the BusinessEntity table
    val businessEntitySchema = StructType(Array(
      StructField("BusinessEntityID", IntegerType, nullable = false),
      StructField("rowguid", StringType, nullable = false),  // UUIDs are stored as strings
      StructField("ModifiedDate", TimestampType, nullable = false)
    ))

    // Define the schema for the Person table
    val personSchema = StructType(Array(
      StructField("BusinessEntityID", IntegerType, nullable = false),
      StructField("PersonType", StringType, nullable = false),  // Use StringType with fixed length constraint externally
      StructField("NameStyle", BooleanType, nullable = false),
      StructField("Title", StringType, nullable = true),  // Nullable
      StructField("FirstName", StringType, nullable = false),
      StructField("MiddleName", StringType, nullable = true),  // Nullable
      StructField("LastName", StringType, nullable = false),
      StructField("Suffix", StringType, nullable = true),  // Nullable
      StructField("EmailPromotion", IntegerType, nullable = false),
      StructField("AdditionalContactInfo", StringType, nullable = true),  // XML data as String for now
      StructField("Demographics", StringType, nullable = true),  // XML data as String for now
      StructField("rowguid", StringType, nullable = false),  // UUID stored as string
      StructField("ModifiedDate", TimestampType, nullable = false)
    ))

    val personDF = spark.read
      .option("delimiter", "\t")
      .schema(personSchema)
      .csv("/home/ahmad/Documents/project/AdventureWorks-oltp-install-script/Person.csv")

    val beDF = spark.read
      .option("delimiter", "\t")
      .schema(businessEntitySchema)
      .csv("/home/ahmad/Documents/project/AdventureWorks-oltp-install-script/BusinessEntity.csv")

    val result = "rm -rf spark-warehouse".!!
    print(result)

    personDF.write.mode("overwrite").saveAsTable("persistent_person")
    beDF.write.mode("overwrite").saveAsTable("persistent_be")

    spark.sql("ANALYZE TABLE persistent_person COMPUTE STATISTICS FOR COLUMNS BusinessEntityID, persontype")
    spark.sql("ANALYZE TABLE persistent_be COMPUTE STATISTICS")

    spark.sql("DESCRIBE EXTENDED persistent_person").show(truncate = false)

    val persistentPersonDF = spark.table("persistent_person")
    val persistentBEDF = spark.table("persistent_person")

    val filteredPersonDF = persistentPersonDF
      .filter(col("BusinessEntityID") <= 120 && col("persontype") === "EM")

    val joinedDF = filteredPersonDF.as("person")
      .join(persistentBEDF.as("be"), $"person.BusinessEntityID" === $"be.BusinessEntityID")
      .select(
        $"person.BusinessEntityID".alias("PersonBusinessEntityID"),
        $"be.BusinessEntityID".alias("BusinessEntityBusinessEntityID"),
        $"person.persontype"
      )

    // Show the result
    joinedDF.explain("cost")
    joinedDF.write
      .mode("overwrite")
      .option("header", "true")
      .csv("data/savedDF")

    print(joinedDF.count())
  }
}

