package utils.files

import scala.io.Source
import java.nio.file.{Files, Paths}

object File {

  def dumpFile(path: String): String = {
    if (Files.exists(Paths.get(path))) {
      val source = Source.fromFile(path)
      try source.mkString
      finally source.close()
    } else {
      throw new java.io.FileNotFoundException(s"File not found: $path")
    }
  }

}
