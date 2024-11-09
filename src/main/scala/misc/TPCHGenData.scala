package misc

import com.databricks.spark.sql.perf.mllib.MLBenchmarks.sqlContext
import com.databricks.spark.sql.perf.tpch.TPCHTables
import org.apache.spark.sql.SparkSession

object TPCHGenData {

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

    val sc = spark.sparkContext
    val schemaMap = Map(
      "part" -> """
  p_partkey int,
  p_name string,
  p_mfgr string,
  p_brand string,
  p_type string,
  p_size int,
  p_container string,
  p_retailprice decimal,
  p_comment string
  """,

      "supplier" -> """
  s_suppkey int,
  s_name string,
  s_address string,
  s_nationkey int,
  s_phone string,
  s_acctbal decimal,
  s_comment string
  """,

      "partsupp" -> """
  ps_partkey int,
  ps_suppkey int,
  ps_availqty int,
  ps_supplycost decimal,
  ps_comment string
  """,

      "customer" -> """
  c_custkey int,
  c_name string,
  c_address string,
  c_nationkey int,
  c_phone string,
  c_acctbal decimal,
  c_mktsegment string,
  c_comment string
  """,

      "orders" -> """
  o_orderkey int,
  o_custkey int,
  o_orderstatus string,
  o_totalprice decimal,
  o_orderdate date,
  o_orderpriority string,
  o_clerk string,
  o_shippriority int,
  o_comment string
  """,

      "lineitem" -> """
  l_orderkey int,
  l_partkey int,
  l_suppkey int,
  l_linenumber int,
  l_quantity decimal,
  l_extendedprice decimal,
  l_discount decimal,
  l_tax decimal,
  l_returnflag string,
  l_linestatus string,
  l_shipdate date,
  l_commitdate date,
  l_receiptdate date,
  l_shipinstruct string,
  l_shipmode string,
  l_comment string
  """,

      "region" -> """
  r_regionkey int,
  r_name string,
  r_comment string
  """,

      "nation" -> """
  n_nationkey int,
  n_name string,
  n_regionkey int,
  n_comment string
  """
    )

    // Set:
    // Note: Here my env is using MapRFS, so I changed it to "hdfs:///tpcds".
    // Note: If you are using HDFS, the format should be like "hdfs://namenode:9000/tpcds"
    val rootDir = "tpch-data-for-main" // root directory of location to create data in.

    val databaseName = "main" // name of database to create.
    val scaleFactor = "1" // scaleFactor defines the size of the dataset to generate (in GB).
    val format = "parquet" // valid spark format like parquet "parquet".
    // Run:
    val tables = new TPCHTables(sqlContext,
      dbgenDir = "/home/ahmad/Documents/project/tpch-dbgen/", // location of dbgen
      scaleFactor = scaleFactor,
      useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
      useStringForDate = false,
      generatorParams = Nil
    ) // true to replace DateType with StringType

//    tableNames.foreach { tableName =>
      // generate data
//      time {
//        tables.genData(
//          location = location,
//          format = fileFormat,
//          overwrite = overwrite,
//          partitionTables = true,
//          // if to coallesce into a single file (only one writter for non partitioned tables = slow)
//          clusterByPartitionColumns = shuffle, //if (isPartitioned(tables, tableName)) false else true,
//          filterOutNullPartitionValues = false,
//          tableFilter = tableName,
//          numPartitions = if (scaleFactor.toInt <= 100 || !isPartitioned(tables, tableName)) (workers * cores) else (workers * cores * 4))
//      }
    sqlContext.sql(s"create database if not exists $databaseName")
    sqlContext.sql(s"use $databaseName")

    tables.genData(
      location = rootDir,
      format = format,
      overwrite = true, // overwrite the data that is already there
      partitionTables = true, // create the partitioned fact tables
      clusterByPartitionColumns = true, // shuffle to get partitions coalesced into single files.
      filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
      tableFilter = "", // "" means generate all tables
      numPartitions = 2) // how many dsdgen partitions to run - number of input tasks.

    // Create the specified database

    tables.tables.foreach { t =>
      val tablename = t.name
      sqlContext.sql(s"""
          CREATE EXTERNAL TABLE IF NOT EXISTS tpch.${tablename} (
          ${schemaMap(tablename)}
          )
          STORED AS PARQUET
          LOCATION '/home/ahmad/Documents/project/cardinality-estimator-test/$rootDir'
      """)
    }

//    tables.createExternalTables(rootDir,
//      format,
//      databaseName,
//      overwrite = true,
//      discoverPartitions = true)

  }
}
