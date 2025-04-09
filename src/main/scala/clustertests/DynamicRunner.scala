package clustertests

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

object DynamicRunner {
  def main(args: Array[String]): Unit = {
    val code =
      """
        |import fuzzer.data.tables.Examples._
        |object Program {
        |  def main(args: Array[String]): Unit = {
        |    println(tpcdsTables(0))
        |  }
        |}
        |Program.main(Array())
      """.stripMargin

    val toolbox = currentMirror.mkToolBox()
    toolbox.eval(toolbox.parse(code))
  }
}
