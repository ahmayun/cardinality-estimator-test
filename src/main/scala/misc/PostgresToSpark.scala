package misc

import org.apache.spark.sql.SparkSession

import java.util.Properties

object PostgresToSpark {
  val dbInfoString =
    """
      |     schema     |                 name                  |    type
      |----------------+---------------------------------------+------------
      | hr             | d                                     | VIEW
      | hr             | e                                     | VIEW
      | hr             | edh                                   | VIEW
      | hr             | eph                                   | VIEW
      | hr             | jc                                    | VIEW
      | hr             | s                                     | VIEW
      | humanresources | department                            | BASE TABLE
      | humanresources | employee                              | BASE TABLE
      | humanresources | employeedepartmenthistory             | BASE TABLE
      | humanresources | employeepayhistory                    | BASE TABLE
      | humanresources | jobcandidate                          | BASE TABLE
      | humanresources | shift                                 | BASE TABLE
      | humanresources | vemployee                             | VIEW
      | humanresources | vemployeedepartment                   | VIEW
      | humanresources | vemployeedepartmenthistory            | VIEW
      | humanresources | vjobcandidate                         | VIEW
      | humanresources | vjobcandidateeducation                | VIEW
      | humanresources | vjobcandidateemployment               | VIEW
      | pe             | a                                     | VIEW
      | pe             | at                                    | VIEW
      | pe             | be                                    | VIEW
      | pe             | bea                                   | VIEW
      | pe             | bec                                   | VIEW
      | pe             | cr                                    | VIEW
      | pe             | ct                                    | VIEW
      | pe             | e                                     | VIEW
      | pe             | p                                     | VIEW
      | pe             | pa                                    | VIEW
      | pe             | pnt                                   | VIEW
      | pe             | pp                                    | VIEW
      | pe             | sp                                    | VIEW
      | person         | address                               | BASE TABLE
      | person         | addresstype                           | BASE TABLE
      | person         | businessentity                        | BASE TABLE
      | person         | businessentityaddress                 | BASE TABLE
      | person         | businessentitycontact                 | BASE TABLE
      | person         | contacttype                           | BASE TABLE
      | person         | countryregion                         | BASE TABLE
      | person         | emailaddress                          | BASE TABLE
      | person         | password                              | BASE TABLE
      | person         | person                                | BASE TABLE
      | person         | personphone                           | BASE TABLE
      | person         | phonenumbertype                       | BASE TABLE
      | person         | stateprovince                         | BASE TABLE
      | person         | vadditionalcontactinfo                | VIEW
      | pr             | bom                                   | VIEW
      | pr             | c                                     | VIEW
      | pr             | d                                     | VIEW
      | pr             | i                                     | VIEW
      | pr             | l                                     | VIEW
      | pr             | p                                     | VIEW
      | pr             | pc                                    | VIEW
      | pr             | pch                                   | VIEW
      | pr             | pd                                    | VIEW
      | pr             | pdoc                                  | VIEW
      | pr             | pi                                    | VIEW
      | pr             | plph                                  | VIEW
      | pr             | pm                                    | VIEW
      | pr             | pmi                                   | VIEW
      | pr             | pmpdc                                 | VIEW
      | pr             | pp                                    | VIEW
      | pr             | ppp                                   | VIEW
      | pr             | pr                                    | VIEW
      | pr             | psc                                   | VIEW
      | pr             | sr                                    | VIEW
      | pr             | th                                    | VIEW
      | pr             | tha                                   | VIEW
      | pr             | um                                    | VIEW
      | pr             | w                                     | VIEW
      | pr             | wr                                    | VIEW
      | production     | billofmaterials                       | BASE TABLE
      | production     | culture                               | BASE TABLE
      | production     | document                              | BASE TABLE
      | production     | illustration                          | BASE TABLE
      | production     | location                              | BASE TABLE
      | production     | product                               | BASE TABLE
      | production     | productcategory                       | BASE TABLE
      | production     | productcosthistory                    | BASE TABLE
      | production     | productdescription                    | BASE TABLE
      | production     | productdocument                       | BASE TABLE
      | production     | productinventory                      | BASE TABLE
      | production     | productlistpricehistory               | BASE TABLE
      | production     | productmodel                          | BASE TABLE
      | production     | productmodelillustration              | BASE TABLE
      | production     | productmodelproductdescriptionculture | BASE TABLE
      | production     | productphoto                          | BASE TABLE
      | production     | productproductphoto                   | BASE TABLE
      | production     | productreview                         | BASE TABLE
      | production     | productsubcategory                    | BASE TABLE
      | production     | scrapreason                           | BASE TABLE
      | production     | transactionhistory                    | BASE TABLE
      | production     | transactionhistoryarchive             | BASE TABLE
      | production     | unitmeasure                           | BASE TABLE
      | production     | workorder                             | BASE TABLE
      | production     | workorderrouting                      | BASE TABLE
      | production     | vproductmodelcatalogdescription       | VIEW
      | production     | vproductmodelinstructions             | VIEW
      | pu             | pod                                   | VIEW
      | pu             | poh                                   | VIEW
      | pu             | pv                                    | VIEW
      | pu             | sm                                    | VIEW
      | pu             | v                                     | VIEW
      | purchasing     | productvendor                         | BASE TABLE
      | purchasing     | purchaseorderdetail                   | BASE TABLE
      | purchasing     | purchaseorderheader                   | BASE TABLE
      | purchasing     | shipmethod                            | BASE TABLE
      | purchasing     | vendor                                | BASE TABLE
      | purchasing     | vvendorwithaddresses                  | VIEW
      | purchasing     | vvendorwithcontacts                   | VIEW
      | sa             | c                                     | VIEW
      | sa             | cc                                    | VIEW
      | sa             | cr                                    | VIEW
      | sa             | crc                                   | VIEW
      | sa             | cu                                    | VIEW
      | sa             | pcc                                   | VIEW
      | sa             | s                                     | VIEW
      | sa             | sci                                   | VIEW
      | sa             | so                                    | VIEW
      | sa             | sod                                   | VIEW
      | sa             | soh                                   | VIEW
      | sa             | sohsr                                 | VIEW
      | sa             | sop                                   | VIEW
      | sa             | sp                                    | VIEW
      | sa             | spqh                                  | VIEW
      | sa             | sr                                    | VIEW
      | sa             | st                                    | VIEW
      | sa             | sth                                   | VIEW
      | sa             | tr                                    | VIEW
      | sales          | countryregioncurrency                 | BASE TABLE
      | sales          | creditcard                            | BASE TABLE
      | sales          | currency                              | BASE TABLE
      | sales          | currencyrate                          | BASE TABLE
      | sales          | customer                              | BASE TABLE
      | sales          | personcreditcard                      | BASE TABLE
      | sales          | salesorderdetail                      | BASE TABLE
      | sales          | salesorderheader                      | BASE TABLE
      | sales          | salesorderheadersalesreason           | BASE TABLE
      | sales          | salesperson                           | BASE TABLE
      | sales          | salespersonquotahistory               | BASE TABLE
      | sales          | salesreason                           | BASE TABLE
      | sales          | salestaxrate                          | BASE TABLE
      | sales          | salesterritory                        | BASE TABLE
      | sales          | salesterritoryhistory                 | BASE TABLE
      | sales          | shoppingcartitem                      | BASE TABLE
      | sales          | specialoffer                          | BASE TABLE
      | sales          | specialofferproduct                   | BASE TABLE
      | sales          | store                                 | BASE TABLE
      | sales          | vindividualcustomer                   | VIEW
      | sales          | vpersondemographics                   | VIEW
      | sales          | vsalesperson                          | VIEW
      | sales          | vsalespersonsalesbyfiscalyears        | VIEW
      | sales          | vsalespersonsalesbyfiscalyearsdata    | VIEW
      | sales          | vstorewithaddresses                   | VIEW
      | sales          | vstorewithcontacts                    | VIEW
      | sales          | vstorewithdemographics                | VIEW
      |""".stripMargin

  def parseDbInfo(input: String): List[(String, String)] = {
    input
      .split("\n") // Split by new lines
      .drop(3) // Drop the header rows and dashes line
      .filter(_.trim.nonEmpty) // Remove any empty or whitespace-only lines
      .map { line =>
        // Split the line by the pipe `|` character and trim each column
        val columns = line.split("\\|").map(_.trim)
        (columns(0), columns(1)) // Extract the schema and name (first two columns)
      }
      .toList // Convert the array to a List of tuples
  }

  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSession.builder()
      .appName("PostgresSparkApp")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val jdbcUrl = "jdbc:postgresql://localhost:5432/Adventureworks"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "ahmad")
    connectionProperties.put("password", "1212")
    connectionProperties.put("driver", "org.postgresql.Driver")

    val tables = parseDbInfo(dbInfoString)

    tables.foreach { case (schema, table) =>
      val tableName = s"$schema.$table"
      println(s"Getting $tableName via JDBC...")
      val df = spark.read.jdbc(jdbcUrl, tableName, connectionProperties)
      println(s"Success!")
      println(s"""Creating database "$schema"...""")
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $schema")
      println(s"Success!")
      println(s"Writing $tableName")
      df.write.saveAsTable(tableName) // Save as a Spark table
      println(s"Success!")
    }
  }
//  def main(args: Array[String]): Unit = {
//    // Create Spark session
//    val spark = SparkSession.builder()
//      .appName("PostgresSparkApp")
//      .master("local[*]")
//      .enableHiveSupport()
//      .getOrCreate()
//
//    val jdbcUrl = "jdbc:postgresql://localhost:5432/Adventureworks"
//    val connectionProperties = new Properties()
//    connectionProperties.put("user", "ahmad")
//    connectionProperties.put("password", "1212")
//    connectionProperties.put("driver", "org.postgresql.Driver")
//
//    val tables = parseDbInfo(dbInfoString)
//
//    tables.foreach { case (schema, table) =>
//      val tableName = s"$schema.$table"
//      println(s"Getting $tableName via JDBC...")
//      val df = spark.read.jdbc(jdbcUrl, tableName, connectionProperties)
//      println(s"Success!")
//
//      // Get the schema as a DDL string
//      val schemaDDL = df.schema.toDDL
//
//      println(s"""Creating database "$schema"...""")
//      spark.sql(s"CREATE DATABASE IF NOT EXISTS $schema")
//      spark.sql(s"USE $schema")
//      println(s"Success!")
//
//      println(s"Writing $tableName")
//      val tablePath = s"/home/ahmad/Documents/project/cardinality-estimator-test/spark-warehouse/$tableName"
//      df.write.mode("overwrite").parquet(tablePath)
//
//      // Create external table with schema
//      println(s"Creating external table $tableName with schema...")
//      spark.sql(s"""
//        CREATE EXTERNAL TABLE IF NOT EXISTS $schema.$table
//        ($schemaDDL)
//        STORED AS PARQUET
//        LOCATION '$tablePath'
//      """)
//
//      spark.catalog.refreshTable(s"$schema.$table")
//
//      println(s"Success!")
//    }
//  }
}
