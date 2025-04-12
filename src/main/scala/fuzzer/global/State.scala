package fuzzer.global

import fuzzer.data.tables.TableMetadata
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object State {

  var iteration: Long = 0
  var src2TableMap: Map[String, TableMetadata] = Map()
  var sparkOption: Option[SparkSession] = None // set at runtime

  var finalDF: Option[DataFrame] = None
}
