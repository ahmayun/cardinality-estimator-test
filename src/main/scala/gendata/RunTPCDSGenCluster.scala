package gendata

object RunTPCDSGenCluster {

  def main(args: Array[String]): Unit = {
    TPCDSGenData.main(Array(
      "spark://zion-headnode:7077",
      "/var/tpc-data/tpc-ds",
      "/home/student/spark-opt/tpcds-kit/tools",
      "100",
      "208"
    ))
  }
}
