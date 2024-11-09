package misc

import org.apache.spark.sql.SparkSession

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ServerSocket, Socket}

object SparkSQLServer {
  var total_queries = 0
  var errors = 0
  var successes = 0

  def main(args: Array[String]): Unit = {
    // Define the port to listen on
    val port = 9999 // You can choose any available port
    val spark = SparkSession.builder()
      .appName("PostgresSparkApp")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val serverSocket = new ServerSocket(port)
    println(s"Server listening on port $port...")

    try {
      // Keep accepting connections
      while (true) {
        val clientSocket = serverSocket.accept()
//        println("Client connected!")

        // Handle client in the same thread (no need for a new thread for this task)
        handleClient(clientSocket, spark)
        println(s"Total Queries: $total_queries")
        println(s"Errors: $errors")
        println(s"Successes: $successes")
      }
    } finally {
      serverSocket.close()
    }
  }

  def handleClient(clientSocket: Socket, spark: SparkSession): Unit = {
    var query: String = null

    try {
      val in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream))
      while ({ query = in.readLine(); query != null }) {
        total_queries+=1
        println(s"Received SQL query: $query")

        try {
          spark.sql(query)
          successes+=1
        } catch {
          case e: Exception =>
            errors+=1
            println(s"SQL ERROR: ${e.getMessage}")
            println("Press Enter to continue...")
            scala.io.StdIn.readLine()
        }
      }
    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
    } finally {
      clientSocket.close()
    }
  }
}
