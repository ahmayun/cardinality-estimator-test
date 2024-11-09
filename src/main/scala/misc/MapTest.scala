package misc

abstract class Rule {

  def applys(): Unit
}

object Rule {
  val ruleApplicationCounts: scala.collection.mutable.Map[String, scala.collection.mutable.Map[Int, Int]] = scala.collection.mutable.Map()

  def incrementRuleCount(ruleName: String, branch: Int): Unit = {
    if (!ruleApplicationCounts.contains(ruleName))
      ruleApplicationCounts(ruleName) = scala.collection.mutable.Map()

    if(!ruleApplicationCounts(ruleName).contains(branch))
      ruleApplicationCounts(ruleName)(branch) = 0

    ruleApplicationCounts(ruleName)(branch) += 1
  }
}


object MapTest {

  def main(args: Array[String]): Unit = {
    Rule.incrementRuleCount("org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints", 0)
    Rule.incrementRuleCount("org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints", 0)
    Rule.incrementRuleCount("org.apache.spark.sql.catalyst.optimizer.ColumnPruning", 4)
    Rule.incrementRuleCount("org.apache.spark.sql.catalyst.optimizer.ColumnPruning", 0)
    Rule.incrementRuleCount("org.apache.spark.sql.catalyst.optimizer.EliminateDistinct", 0)
    Rule.incrementRuleCount("org.apache.spark.sql.catalyst.optimizer.PushDownPredicates", 0)
    Rule.incrementRuleCount("org.apache.spark.sql.catalyst.optimizer.PushDownPredicates", 0)
    Rule.incrementRuleCount("org.apache.spark.sql.catalyst.optimizer.PushDownPredicates", 0)
    Rule.incrementRuleCount("org.apache.spark.sql.catalyst.optimizer.PushDownPredicates", 0)
    Rule.incrementRuleCount("org.apache.spark.sql.catalyst.optimizer.PushDownPredicates", 0)
    Rule.incrementRuleCount("org.apache.spark.sql.catalyst.optimizer.PushDownPredicates", 0)
    Rule.incrementRuleCount("org.apache.spark.sql.catalyst.optimizer.PushDownPredicates", 0)
    Rule.incrementRuleCount("org.apache.spark.sql.catalyst.optimizer.EliminateSorts", 4)
    Rule.incrementRuleCount("org.apache.spark.sql.catalyst.optimizer.CollapseProject", 0)
    Rule.ruleApplicationCounts.foreach {
      case (ruleName, pathMap) =>
        println(s"$ruleName, ${pathMap.getClass}")
    }
  }

}
