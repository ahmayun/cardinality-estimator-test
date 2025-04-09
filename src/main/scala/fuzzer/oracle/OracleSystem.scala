package fuzzer.oracle

import org.apache.spark.sql.AnalysisException
import java.lang.reflect.InvocationTargetException
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

object OracleSystem {
  def check(source: String): String = {
    val toolbox = currentMirror.mkToolBox()
    try {
      toolbox.eval(toolbox.parse(source))
      "SUCCESS"
    } catch {
      case e: InvocationTargetException =>
        e.getCause match {
          case e =>
            val res = e.getClass.toString.split('.').last
            res
        }
      case e: Exception =>
        e.printStackTrace()
        sys.error("Dynamic program invocation failed in OracleSystem.check()")
    }
  }
}
