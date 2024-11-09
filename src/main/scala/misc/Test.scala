package misc

import sqlsmith.loader.SQLSmithLoader

object Test {

  def main(args: Array[String]): Unit = {
    lazy val sqlSmithApi = SQLSmithLoader.loadApi()
    val sqlSmithSchema = sqlSmithApi.schemaInit("", 0)

    println(s"Test ran with ${args.mkString("Array(", ", ", ")")}")
    println(s"Successfully loaded sqlsmith JNI ${sqlSmithSchema}")
  }
}
