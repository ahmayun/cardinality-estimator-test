package diff

import org.apache.spark.sql.SparkSession

import java.io.IOException
import java.nio.file._
import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties
import scala.io.Source
import scala.util.Try


object CEPostgreSQL {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .master("local[*]")
    .getOrCreate()


  def analyzeSQLQuery(viewName: String, jdbcUrl: String, user: String, password: String): Int = {
    val conn: Connection = DriverManager.getConnection(jdbcUrl, user, password)
    var cardinality: Int = 0
    try {
      val statement = conn.createStatement()
      statement.execute("SET search_path TO person, public")

      // Run ANALYZE command to update statistics
      statement.executeUpdate(s"ANALYZE $viewName")
      // Use EXPLAIN to estimate the cardinality
      val resultSet = statement.executeQuery(s"EXPLAIN SELECT * FROM $viewName")
      while (resultSet.next()) {
        val rowsEstimate = resultSet.getString(1)
        val pattern = """rows=(\d+)""".r
        return pattern.findFirstMatchIn(rowsEstimate) match {
          case Some(m) => m.group(1).toInt
          case None => -1
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
      conn.close()
    }
    cardinality
  }


  def explainSQLQuery(query: String, jdbcUrl: String, user: String, password: String): Unit = {
    val conn: Connection = DriverManager.getConnection(jdbcUrl, user, password)
    try {
      val statement = conn.createStatement()
      statement.execute("SET search_path TO person, public")

      val resultSet = statement.executeQuery(s"EXPLAIN $query")
      while (resultSet.next()) {
        val explanation = resultSet.getString(1)
        println(explanation)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      conn.close()
    }
  }


  def main(args: Array[String]): Unit = {
    val jdbcUrl = "jdbc:postgresql://localhost:5432/Adventureworks"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "ahmad")
    connectionProperties.put("password", "1212")
    connectionProperties.put("driver", "org.postgresql.Driver")

    val conn = DriverManager.getConnection(jdbcUrl, "ahmad", "1212")
    val query = "select p.BusinessEntityID AS PersonBusinessEntityID, be.BusinessEntityID AS BusinessEntityBusinessEntityID, p.persontype from person.person p inner join person.BusinessEntity be on p.BusinessEntityID=be.BusinessEntityID where p.BusinessEntityID <= 120 AND p.persontype = 'EM';"

    println(s"Original Query:\n$query")

    try {
      val viewName = "view2"
      val modifiedQuery = s"CREATE VIEW $viewName AS $query"
      val statement = conn.createStatement()
      println("Executing query:")
      statement.execute("SET search_path TO person, public")
      statement.executeUpdate(modifiedQuery)
      println(s"Executed successfully")

      println("Explaining query:")
      explainSQLQuery(query, jdbcUrl, "ahmad", "1212")

      println("Getting cardinality:")
      val cardinality = analyzeSQLQuery(viewName, jdbcUrl, "ahmad", "1212")
      println(s"Cardinality Estimate: $cardinality")

      println("Counting:")
      val countQuery = query.replaceFirst("(?i)select .* from", "SELECT COUNT(*) FROM")
      val resultSet = statement.executeQuery(countQuery)
      if (resultSet.next()) {
        println(s"Count: ${resultSet.getInt(1)}")
      }


      println(s"Dropping view $viewName")
      val dropQuery = s"DROP VIEW IF EXISTS $viewName"
      statement.executeUpdate(dropQuery)
      println("Dropped successfully")

    } finally {
      conn.close()
    }
  }


  def loadMutations(path: String): Map[String, List[String]] = {
    val source = Source.fromFile(path)
    try {
      source.getLines().drop(1).foldLeft(Map.empty[String, List[String]]) { (acc, line) =>
        val Array(action, mutation) = line.split(",").map(_.trim.toLowerCase) // Normalize to lower case when loading
        acc + (action -> (acc.getOrElse(action, List.empty) :+ mutation))
      }
    } finally {
      source.close()
    }
  }


  def applyMutations(query: String, mutations: Map[String, List[String]]): Seq[String] = {
    val mutationQueries = scala.collection.mutable.ListBuffer[String]()

    mutations.foreach { case (key, mutationsList) =>
      val keyPattern = s"(?i)\\b$key\\b".r
      val matchData = keyPattern.findAllMatchIn(query).toList


      matchData.foreach { matchInstance =>
        mutationsList.foreach { mutation =>

          val mutatedQuery = query.substring(0, matchInstance.start) + mutation + query.substring(matchInstance.end)
          mutationQueries += mutatedQuery
        }
      }
    }

    mutationQueries.toList.distinct
  }


  def deleteContents(directory: Path): Unit = {
    Try {
      Files.walk(directory)
        .sorted((path1, path2) => -path1.compareTo(path2))
        .forEach { path =>
          try {
            if (Files.isDirectory(path)) {
              Files.delete(path)
            } else {
              Files.deleteIfExists(path)
            }
          } catch {
            case e: DirectoryNotEmptyException =>
              println(s"Directory not empty: $path - ${e.getMessage}")
            case e: NoSuchFileException =>
              println(s"No such file/directory: $path - ${e.getMessage}")
            case e: IOException =>
              println(s"IO Exception when deleting: $path - ${e.getMessage}")
              e.printStackTrace()
            case e: SecurityException =>
              println(s"Security Exception when deleting: $path - ${e.getMessage}")
          }
        }
    }.recover {
      case e: Exception =>
        println(s"Error walking through $directory: ${e.getMessage}")
    }
  }
}









