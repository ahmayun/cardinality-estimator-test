package diff

import org.apache.spark.sql.SparkSession

import scala.io.Source

import scala.util.Try
import java.nio.file._
import java.io.IOException
import java.nio.file.{Files, Path, Paths}

import java.util.Properties
import java.sql.{Connection, DriverManager, ResultSet, SQLException}




object SQLTest {

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
        pattern.findFirstMatchIn(rowsEstimate) match {
          case Some(m) => cardinality = m.group(1).toInt
          case None => println("No cardinality estimate found.")
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

    val query_to_test_database = "select p.BusinessEntityID AS PersonBusinessEntityID, be.BusinessEntityID AS BusinessEntityBusinessEntityID, p.persontype from person p inner join BusinessEntity be on p.BusinessEntityID=be.BusinessEntityID where p.BusinessEntityID <= 120 AND p.persontype = 'EM';"
    explainSQLQuery(query_to_test_database, jdbcUrl, "ahmad","1212")

    val mutations = loadMutations("data/allpossiblemutations.csv")
    val query = "select p.BusinessEntityID AS PersonBusinessEntityID, be.BusinessEntityID AS BusinessEntityBusinessEntityID, p.persontype from person.person p inner join person.BusinessEntity be on p.BusinessEntityID=be.BusinessEntityID where p.BusinessEntityID <= 120 AND p.persontype = 'EM';"
    val conn: Connection = DriverManager.getConnection(jdbcUrl, "ahmad", "1212")

//    val testquery = "(select * from person.person LIMIT 10) as person_data"
//    val personDF = spark.read.jdbc(jdbcUrl, testquery, connectionProperties)
//    personDF.show()

//    val results = spark.sql(query)

    println(s"Original Query: $query")
    println("Mutations:")
    var queries = applyMutations(query, mutations)

    val additionalQueries = Seq(
      s"select p.BusinessEntityID AS PersonBusinessEntityID, be.BusinessEntityID AS BusinessEntityBusinessEntityID, p.persontype from person.person p inner join person.BusinessEntity be on p.BusinessEntityID=be.BusinessEntityID where p.BusinessEntityID <= 120 AND p.persontype = 'EM'",
      s"select p.BusinessEntityID AS PersonBusinessEntityID, be.BusinessEntityID AS BusinessEntityBusinessEntityID, p.persontype from person.person p inner join person.BusinessEntity be on p.BusinessEntityID=be.BusinessEntityID where p.BusinessEntityID > 120 AND p.persontype = 'EM'",
      s"select p.BusinessEntityID AS PersonBusinessEntityID, be.BusinessEntityID AS BusinessEntityBusinessEntityID, p.persontype from person.person p inner join person.BusinessEntity be on p.BusinessEntityID=be.BusinessEntityID where p.BusinessEntityID >= 120 AND p.persontype = 'EM'",
      s"select p.BusinessEntityID AS PersonBusinessEntityID, be.BusinessEntityID AS BusinessEntityBusinessEntityID, p.persontype from person.person p inner join person.BusinessEntity be on p.BusinessEntityID=be.BusinessEntityID where p.BusinessEntityID = 120 AND p.persontype = 'EM'",
      s"select p.BusinessEntityID AS PersonBusinessEntityID, be.BusinessEntityID AS BusinessEntityBusinessEntityID, p.persontype from person.person p inner join person.BusinessEntity be on p.BusinessEntityID=be.BusinessEntityID where p.BusinessEntityID <> 120 AND p.persontype = 'EM'",
    )
    queries = queries ++ additionalQueries


    val countQueries = queries.map { query =>
      query.replaceFirst("(?i)select .* from", "SELECT COUNT(*) FROM")
    }

    try {
      val statement = conn.createStatement()
      statement.execute("SET search_path TO person, public")

      countQueries.foreach { query =>
        try {
          val resultSet = statement.executeQuery(query)
          if (resultSet.next()) {
            println(s"Executed successfully: $query, Count: ${resultSet.getInt(1)}")
          }
        } catch {
          case e: SQLException =>
            println(s"Error executing query: $query")
            e.printStackTrace()
        }
      }
    }
    var mutationNumber = 1

    val modifiedQueries = queries.map { currentQuery =>
      val modifiedQuery = s"CREATE VIEW mutation$mutationNumber AS $currentQuery"
      mutationNumber += 1
      modifiedQuery
    }

    try {
      val statement = conn.createStatement()

      statement.execute("SET search_path TO person, public")
      modifiedQueries.foreach { query =>
        try {
          statement.executeUpdate(query)
          println(s"Executed successfully: $query")
        } catch {
          case e: SQLException =>
            println(s"Error executing query: $query")
            e.printStackTrace()
        }
      }
    }

    (1 until mutationNumber).foreach { i =>
      val selectQuery = s"SELECT * FROM mutation$i"
      explainSQLQuery(selectQuery, jdbcUrl, "ahmad", "1212")
    }

    val cardinalityResults = (1 until mutationNumber).map { i =>
      val viewName = s"mutation$i"
      val cardinality = analyzeSQLQuery(viewName, jdbcUrl, "ahmad", "1212")
      (viewName, cardinality)
    }

    cardinalityResults.foreach { case (viewName, cardinality) =>
      println(s"View: $viewName, Cardinality: $cardinality")
    }

    try {
      val statement = conn.createStatement()
      statement.execute("SET search_path TO person, public")

      (1 until mutationNumber).foreach { i =>
        val dropQuery = s"DROP VIEW IF EXISTS mutation$i"
        try {
          statement.executeUpdate(dropQuery)
          println(s"View mutation$i deleted successfully")
        } catch {
          case e: SQLException =>
            println(s"Error deleting view: mutation$i")
            e.printStackTrace()
        }
      }
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









