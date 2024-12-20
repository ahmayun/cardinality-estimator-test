package gendata

import com.databricks.spark.sql.perf.mllib.MLBenchmarks.sqlContext
import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import org.apache.spark.sql.SparkSession
import java.io.File

object TPCDSGenData {
  def printCurrentDirectory(): Unit = {
    val currentDir = new File(".").getCanonicalPath
    println(s"Current Working Directory: $currentDir")
  }

  def printDirectoryContents(dirPath: String): Unit = {
    val directory = new File(dirPath)
    if (directory.exists && directory.isDirectory) {
      val files = directory.listFiles
      println(s"Contents of Directory '$dirPath':")
      files.foreach(file => println(file.getName))
    } else {
      println(s"The path '$dirPath' is not a valid directory.")
    }
  }
  def main(args: Array[String]): Unit = {

    if(args.isEmpty) {
      args(0) = "local[*]"
      args(1) = "tpcds-data-for-main"
      args(2) = "/home/ahmad/Documents/project/tpcds-kit/tools"
      args(3) = "1"
      args(4) = "2"
    }

    val master = args(0)
    val rootDirArg = args(1)
    val dsdgenDirPath = args(2)
    val scaleFactor = args(3)
    val partitions = args(4).toInt

    printCurrentDirectory()
    printDirectoryContents(dsdgenDirPath)
//    sys.exit(0)

    val spark = SparkSession.builder()
      .appName("TPC-DS Data Gen")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    // Set:
    // Note: Here my env is using MapRFS, so I changed it to "hdfs:///tpcds".
    // Note: If you are using HDFS, the format should be like "hdfs://namenode:9000/tpcds"
    val rootDir = rootDirArg // root directory of location to create data in.

    val databaseName = "main" // name of database to create.
    val format = "parquet" // valid spark format like parquet "parquet".
    // Run:
    val tables = new TPCDSTables(sqlContext,
      dsdgenDir = dsdgenDirPath, // location of dsdgen
      scaleFactor = scaleFactor,
      useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
      useStringForDate = false) // true to replace DateType with StringType


//    tables.genData(
//      location = rootDir,
//      format = format,
//      overwrite = true, // overwrite the data that is already there
//      partitionTables = true, // create the partitioned fact tables
//      clusterByPartitionColumns = true, // shuffle to get partitions coalesced into single files.
//      filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
//      tableFilter = "", // "" means generate all tables
//      numPartitions = partitions) // how many dsdgen partitions to run - number of input tasks.

    // Create the specified database
//    sqlContext.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")

    tables.createExternalTables(rootDir, "parquet", databaseName, overwrite = true, discoverPartitions = true)

    // Or, if you want to create temporary tables
    // tables.createTemporaryTables(location, format)

    // For CBO only, gather statistics on all columns:
//    tables.analyzeTables(databaseName, analyzeColumns = true)
//    val tpcds = new TPCDS (sqlContext = sqlContext)
//    val queries = tpcds.tpcds2_4Queries
//    queries.foreach{
//      q =>
//        println(q.sqlText)
//    }
  }
}
