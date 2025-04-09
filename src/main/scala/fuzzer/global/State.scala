package fuzzer.global

import fuzzer.data.tables.TableMetadata
import org.apache.spark.sql.SparkSession

object State {

  var iteration: Long = 0
  var src2TableMap: Map[String, TableMetadata] = Map()
  var spark: SparkSession = null // set at runtime
}
