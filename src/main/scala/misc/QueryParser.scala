package misc

import scala.io.Source

object QueryParser {

  def getQueries(filePath: String): List[String] = {
    // Initialize the source to None and manage it manually
    var source: Option[Source] = None

    try {
      // Open the file
      source = Some(Source.fromFile(filePath))
      // Read the entire file content
      val content = source.get.getLines().mkString(" ")
      // Split the content by semicolon and trim any whitespace
      content.split(";").map(_.trim).filter(_.nonEmpty).toList
    } finally {
      // Ensure the file is closed after processing
      source.foreach(_.close())
    }
  }


  def getQueriesLazy(filePath: String): Iterator[String] = {
    // Open the source lazily using an iterator
    val source = Source.fromFile(filePath)

    // Create an iterator that processes the file line by line
    new Iterator[String] {
      private val sb = new StringBuilder
      private val lines = source.getLines()

      override def hasNext: Boolean = lines.hasNext || sb.nonEmpty

      override def next(): String = {
        while (lines.hasNext) {
          val line = lines.next()
          sb.append(line) // Accumulate lines until a semicolon is found

          if (line.contains(";")) {
            // Split by semicolon, but only return the first query part
            val splitQueries = sb.toString().split(";").map(_.trim).filter(_.nonEmpty)
            sb.clear()
            sb.append(splitQueries.lastOption.getOrElse(""))
            return splitQueries.head
          }
        }

        // If no more lines and StringBuilder still has data, return the remaining query
        if (sb.nonEmpty) {
          val remainingQuery = sb.toString().trim
          sb.clear()
          remainingQuery
        } else {
          throw new NoSuchElementException("No more queries")
        }
      }
    }.takeWhile(_.nonEmpty) // Ensure to only return non-empty queries
  }



  def main(args: Array[String]): Unit = {
    val filePath = "/home/ahmad/Documents/project/sqlsmith/sqlite3_testdb1.log"
    val queries = getQueriesLazy(filePath)

    // Print the queries for debugging purposes
    queries.foreach {
      q =>
        println(q)
        println("____________________________")
    }
    println(s"Num queries: ${queries.length}")
  }
}

