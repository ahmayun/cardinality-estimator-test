package gendata

object RunTPCDSGenCluster {

  def main(args: Array[String]): Unit = {
    TPCDSGenData.main(Array(
      "spark://zion-headnode:7077",
      "hdfs://tpcds/",
      "/home/student/spark-opt/tpcds-kit/tools"
    ))
  }
}
