package fuzzer.global
import java.io.File
import scala.io.Source
import play.api.libs.json._

case class FuzzerConfig(
                         master: String = "local[*]",
                         exitAfterNSuccesses: Boolean = true,
                         N: Int = 200,
                         d: Int = 200,
                         p: Int = 10,
                         outDir: String = "./out",
                         outExt: String = ".scala",
                         timeLimitSec: Int = 10,
                         dagGenDir: String = "dag-gen/DAGs/DAGs",
                         localTpcdsPath: String = "tpcds-data",
                         seed: Int = "ahmad35".hashCode,
                         maxStringLength: Int = 5,
                         updateLiveStatsAfter: Int = 10,
                         intermediateVarPrefix: String = "auto",
                         finalVariableName: String = "sink",
                         probUDFInsert: Double = 0.1,
                         maxListLength: Int = 2,
                         randIntMin: Int = -50,
                         randIntMax: Int = 50,
                         randFloatMin: Double = -50.0,
                         randFloatMax: Double = 50.0,
                         logicalOperatorSet: Set[String] = Set(">", "<", ">=", "<=")
                       )

object FuzzerConfig {
  implicit val configReads: Reads[FuzzerConfig] = Json.reads[FuzzerConfig]

  var config: FuzzerConfig = FuzzerConfig()

  def fromJsonFile(path: String): FuzzerConfig = {
    val fileContents = Source.fromFile(new File(path)).getLines().mkString
    val conf = Json.parse(fileContents).as[FuzzerConfig]
    config = conf
    conf
  }

  def getDefault: FuzzerConfig = {
    config = FuzzerConfig()
    config
  }

}