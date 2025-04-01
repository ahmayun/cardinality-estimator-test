//package org.apache.spark.sql.catalyst.optimizer
//import scala.collection.mutable
//import org.apache.spark.sql.AnalysisException
//import org.apache.spark.sql.catalyst.analysis._
//import org.apache.spark.sql.catalyst.catalog.{ InMemoryCatalog, SessionCatalog }
//import org.apache.spark.sql.catalyst.expressions._
//import org.apache.spark.sql.catalyst.expressions.aggregate._
//import org.apache.spark.sql.catalyst.plans._
//import org.apache.spark.sql.catalyst.plans.logical._
//import org.apache.spark.sql.catalyst.rules._
//import org.apache.spark.sql.connector.catalog.CatalogManager
//import org.apache.spark.sql.internal.SQLConf
//import org.apache.spark.sql.types._
//import org.apache.spark.util.Utils
//abstract class Optimizer(catalogManager: CatalogManager) extends RuleExecutor[LogicalPlan] {
//  override protected def isPlanIntegral(plan: LogicalPlan): Boolean = {
//    !Utils.isTesting || plan.resolved && plan.find(PlanHelper.specialExpressionsInUnsupportedOperator(_).nonEmpty).isEmpty
//  }
//  override protected val blacklistedOnceBatches: Set[String] = Set("PartitionPruning", "Extract Python UDFs")
//  protected def fixedPoint = FixedPoint(SQLConf.get.optimizerMaxIterations, maxIterationsSetting = SQLConf.OPTIMIZER_MAX_ITERATIONS.key)
//  def defaultBatches: Seq[Batch] = {
//    val operatorOptimizationRuleSet = Seq(PushProjectionThroughUnion, ReorderJoin, EliminateOuterJoin, PushDownPredicates, PushDownLeftSemiAntiJoin, PushLeftSemiLeftAntiThroughJoin, LimitPushDown, ColumnPruning, InferFiltersFromConstraints, CollapseRepartition, CollapseProject, CollapseWindow, CombineFilters, CombineLimits, CombineUnions, TransposeWindow, NullPropagation, ConstantPropagation, FoldablePropagation, OptimizeIn, ConstantFolding, ReorderAssociativeOperator, LikeSimplification, BooleanSimplification, SimplifyConditionals, RemoveDispensableExpressions, SimplifyBinaryComparison, ReplaceNullWithFalseInPredicate, PruneFilters, SimplifyCasts, SimplifyCaseConversionExpressions, RewriteCorrelatedScalarSubquery, EliminateSerialization, RemoveRedundantAliases, RemoveNoopOperators, SimplifyExtractValueOps, CombineConcats) ++ extendedOperatorOptimizationRules
//    val operatorOptimizationBatch: Seq[Batch] = {
//      val rulesWithoutInferFiltersFromConstraints = operatorOptimizationRuleSet.filterNot(_ == InferFiltersFromConstraints)
//      Batch("Operator Optimization before Inferring Filters", fixedPoint, rulesWithoutInferFiltersFromConstraints: _*) :: Batch("Infer Filters", Once, InferFiltersFromConstraints) :: Batch("Operator Optimization after Inferring Filters", fixedPoint, rulesWithoutInferFiltersFromConstraints: _*) :: Nil
//    }
//    val batches = (Batch("Eliminate Distinct", Once, EliminateDistinct) :: Batch("Finish Analysis", Once, EliminateResolvedHint, EliminateSubqueryAliases, EliminateView, ReplaceExpressions, RewriteNonCorrelatedExists, ComputeCurrentTime, GetCurrentDatabase(catalogManager), RewriteDistinctAggregates, ReplaceDeduplicateWithAggregate) :: Batch("Union", Once, CombineUnions) :: Batch("OptimizeLimitZero", Once, OptimizeLimitZero) :: Batch("LocalRelation early", fixedPoint, ConvertToLocalRelation, PropagateEmptyRelation) :: Batch("Pullup Correlated Expressions", Once, PullupCorrelatedPredicates) :: Batch("Subquery", FixedPoint(1), OptimizeSubqueries) :: Batch("Replace Operators", fixedPoint, RewriteExceptAll, RewriteIntersectAll, ReplaceIntersectWithSemiJoin, ReplaceExceptWithFilter, ReplaceExceptWithAntiJoin, ReplaceDistinctWithAggregate) :: Batch("Aggregate", fixedPoint, RemoveLiteralFromGroupExpressions, RemoveRepetitionFromGroupExpressions) :: (Nil ++ operatorOptimizationBatch)) :+ Batch("Early Filter and Projection Push-Down", Once, earlyScanPushDownRules: _*) :+ Batch("Join Reorder", FixedPoint(1), CostBasedJoinReorder) :+ Batch("Eliminate Sorts", Once, EliminateSorts) :+ Batch("Decimal Optimizations", fixedPoint, DecimalAggregates) :+ Batch("Object Expressions Optimization", fixedPoint, EliminateMapObjects, CombineTypedFilters, ObjectSerializerPruning, ReassignLambdaVariableID) :+ Batch("LocalRelation", fixedPoint, ConvertToLocalRelation, PropagateEmptyRelation) :+ Batch("Check Cartesian Products", Once, CheckCartesianProducts) :+ Batch("RewriteSubquery", Once, RewritePredicateSubquery, ColumnPruning, CollapseProject, RemoveNoopOperators) :+ Batch("NormalizeFloatingNumbers", Once, NormalizeFloatingNumbers)
//    batches.filter(_.rules.nonEmpty)
//  }
//  def nonExcludableRules: Seq[String] = EliminateDistinct.ruleName :: EliminateResolvedHint.ruleName :: EliminateSubqueryAliases.ruleName :: EliminateView.ruleName :: ReplaceExpressions.ruleName :: ComputeCurrentTime.ruleName :: GetCurrentDatabase(catalogManager).ruleName :: RewriteDistinctAggregates.ruleName :: ReplaceDeduplicateWithAggregate.ruleName :: ReplaceIntersectWithSemiJoin.ruleName :: ReplaceExceptWithFilter.ruleName :: ReplaceExceptWithAntiJoin.ruleName :: RewriteExceptAll.ruleName :: RewriteIntersectAll.ruleName :: ReplaceDistinctWithAggregate.ruleName :: PullupCorrelatedPredicates.ruleName :: RewriteCorrelatedScalarSubquery.ruleName :: RewritePredicateSubquery.ruleName :: NormalizeFloatingNumbers.ruleName :: Nil
//  object OptimizeSubqueries extends Rule[LogicalPlan] {
//    private def removeTopLevelSort(plan: LogicalPlan): LogicalPlan = {
//      plan match {
//        case Sort(_, _, child) =>
//          println("Hello")
//          child
//        case Project(fields, child) =>
//          println("Hello")
//          Project(fields, removeTopLevelSort(child))
//        case other =>
//          println("Hello")
//          other
//      }
//    }
//    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
//      case s: SubqueryExpression =>
//        val Subquery(newPlan, _) = Optimizer.this.execute(Subquery.fromExpression(s))
//        Rule.incrementRuleCount(this.ruleName, 0)
//        s.withNewPlan(removeTopLevelSort(newPlan))
//    }
//  }
//  def extendedOperatorOptimizationRules: Seq[Rule[LogicalPlan]] = Nil
//  def earlyScanPushDownRules: Seq[Rule[LogicalPlan]] = Nil
//  final override def batches: Seq[Batch] = {
//    val excludedRulesConf = SQLConf.get.optimizerExcludedRules.toSeq.flatMap(Utils.stringToSeq)
//    val excludedRules = excludedRulesConf.filter { ruleName =>
//      val nonExcludable = nonExcludableRules.contains(ruleName)
//      if (nonExcludable) {
//        println("Hello")
//        logWarning(s"Optimization rule '${
//          ruleName
//        }' was not excluded from the optimizer " + (s"because this rule is a non-excludable rule."))
//      } else {
//        println("Hello")
//      }
//      !nonExcludable
//    }
//    if (excludedRules.isEmpty) {
//      println("Hello")
//      defaultBatches
//    } else {
//      println("Hello")
//      defaultBatches.flatMap { batch =>
//        val filteredRules = batch.rules.filter { rule =>
//          val exclude = excludedRules.contains(rule.ruleName)
//          if (exclude) {
//            println("Hello")
//            logInfo(s"Optimization rule '${
//              rule.ruleName
//            }' is excluded from the optimizer.")
//          } else {
//            println("Hello")
//          }
//          !exclude
//        }
//        if (batch.rules == filteredRules) {
//          println("Hello")
//          Some(batch)
//        } else if (filteredRules.nonEmpty) {
//          println("Hello")
//          println("Hello")
//          Some(Batch(batch.name, batch.strategy, filteredRules: _*))
//        } else {
//          println("Hello")
//          println("Hello")
//          logInfo(s"Optimization batch '${
//            batch.name
//          }' is excluded from the optimizer " + (s"as all enclosed rules have been excluded."))
//          None
//        }
//      }
//    }
//  }
//}
//object EliminateDistinct extends Rule[LogicalPlan] {
//  override def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
//    case ae: AggregateExpression if ae.isDistinct =>
//      ae.aggregateFunction match {
//        case _: Max | _: Min =>
//          println("Hello")
//          Rule.incrementRuleCount(this.ruleName, 0)
//          ae.copy(isDistinct = false)
//        case _ =>
//          println("Hello")
//          ae
//      }
//  }
//}
//object SimpleTestOptimizer extends SimpleTestOptimizer
//class SimpleTestOptimizer extends Optimizer(new CatalogManager(new SQLConf().copy(SQLConf.CASE_SENSITIVE -> true), FakeV2SessionCatalog, new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, new SQLConf())))
//object RemoveRedundantAliases extends Rule[LogicalPlan] {
//  private def createAttributeMapping(current: LogicalPlan, next: LogicalPlan): Seq[(Attribute, Attribute)] = {
//    current.output.zip(next.output).filterNot {
//      case (a1, a2) =>
//        a1.semanticEquals(a2)
//    }
//  }
//  private def removeRedundantAlias(e: Expression, blacklist: AttributeSet): Expression = e match {
//    case a @ Alias(attr: Attribute, name) if a.metadata == Metadata.empty && name == attr.name && !blacklist.contains(attr) && !blacklist.contains(a) =>
//      println("Hello")
//      attr
//    case a =>
//      println("Hello")
//      a
//  }
//  private def removeRedundantAliases(plan: LogicalPlan, blacklist: AttributeSet): LogicalPlan = {
//    plan match {
//      case Subquery(child, correlated) =>
//        println("Hello")
//        Subquery(removeRedundantAliases(child, blacklist ++ child.outputSet), correlated)
//      case Join(left, right, joinType, condition, hint) =>
//        println("Hello")
//        val newLeft = removeRedundantAliases(left, blacklist ++ right.outputSet)
//        val newRight = removeRedundantAliases(right, blacklist ++ newLeft.outputSet)
//        val mapping = AttributeMap(createAttributeMapping(left, newLeft) ++ createAttributeMapping(right, newRight))
//        val newCondition = condition.map(_.transform {
//          case a: Attribute =>
//            mapping.getOrElse(a, a)
//        })
//        Join(newLeft, newRight, joinType, newCondition, hint)
//      case _ =>
//        println("Hello")
//        val currentNextAttrPairs = mutable.Buffer.empty[(Attribute, Attribute)]
//        val newNode = plan.mapChildren { child =>
//          val newChild = removeRedundantAliases(child, blacklist)
//          currentNextAttrPairs ++= createAttributeMapping(child, newChild)
//          newChild
//        }
//        val mapping = AttributeMap(currentNextAttrPairs)
//        val clean: Expression => Expression = plan match {
//          case _: Project =>
//            println("Hello")
//            removeRedundantAlias(_, blacklist)
//          case _: Aggregate =>
//            println("Hello")
//            removeRedundantAlias(_, blacklist)
//          case _: Window =>
//            println("Hello")
//            removeRedundantAlias(_, blacklist)
//          case _ =>
//            println("Hello")
//            identity[Expression]
//        }
//        newNode.mapExpressions {
//          expr => clean(expr.transform {
//            case a: Attribute =>
//              mapping.getOrElse(a, a)
//          })
//        }
//    }
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = removeRedundantAliases(plan, AttributeSet.empty)
//}
//object RemoveNoopOperators extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case p @ Project(_, child) if child.sameOutput(p) => child
//    case w: Window if w.windowExpressions.isEmpty =>
//      w.child
//  }
//}
//object LimitPushDown extends Rule[LogicalPlan] {
//  private def stripGlobalLimitIfPresent(plan: LogicalPlan): LogicalPlan = {
//    plan match {
//      case GlobalLimit(_, child) =>
//        println("Hello")
//        child
//      case _ =>
//        println("Hello")
//        plan
//    }
//  }
//  private def maybePushLocalLimit(limitExp: Expression, plan: LogicalPlan): LogicalPlan = {
//    (limitExp, plan.maxRowsPerPartition) match {
//      case (IntegerLiteral(newLimit), Some(childMaxRows)) if newLimit < childMaxRows =>
//        println("Hello")
//        LocalLimit(limitExp, stripGlobalLimitIfPresent(plan))
//      case (_, None) =>
//        println("Hello")
//        LocalLimit(limitExp, stripGlobalLimitIfPresent(plan))
//      case _ =>
//        println("Hello")
//        plan
//    }
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case LocalLimit(exp, Union(children)) =>
//      LocalLimit(exp, Union(children.map(maybePushLocalLimit(exp, _))))
//    case LocalLimit(exp, join @ Join(left, right, joinType, _, _)) =>
//      val newJoin = joinType match {
//        case RightOuter =>
//          println("Hello")
//          join.copy(right = maybePushLocalLimit(exp, right))
//        case LeftOuter =>
//          println("Hello")
//          join.copy(left = maybePushLocalLimit(exp, left))
//        case _ =>
//          println("Hello")
//          join
//      }
//      LocalLimit(exp, newJoin)
//  }
//}
//object PushProjectionThroughUnion extends Rule[LogicalPlan] with PredicateHelper {
//  private def buildRewrites(left: LogicalPlan, right: LogicalPlan): AttributeMap[Attribute] = {
//    assert(left.output.size == right.output.size)
//    AttributeMap(left.output.zip(right.output))
//  }
//  private def pushToRight[A <: Expression](e: A, rewrites: AttributeMap[Attribute]) = {
//    val result = e transform {
//      case a: Attribute =>
//        rewrites(a)
//    } match {
//      case Alias(child, alias) =>
//        println("Hello")
//        Alias(child, alias)()
//      case other =>
//        println("Hello")
//        other
//    }
//    result.asInstanceOf[A]
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case p @ Project(projectList, Union(children)) =>
//      assert(children.nonEmpty)
//      if (projectList.forall(_.deterministic)) {
//        println("Hello")
//        val newFirstChild = Project(projectList, children.head)
//        val newOtherChildren = children.tail.map { child =>
//          val rewrites = buildRewrites(children.head, child)
//          Project(projectList.map(pushToRight(_, rewrites)), child)
//        }
//        Rule.incrementRuleCount(this.ruleName, 0)
//        Union(newFirstChild +: newOtherChildren)
//      } else {
//        println("Hello")
//        p
//      }
//  }
//}
//object ColumnPruning extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = removeProjectBeforeFilter(plan transform {
//    case p @ Project(_, p2: Project) if !p2.outputSet.subsetOf(p.references) =>
//      Rule.incrementRuleCount(this.ruleName, 0)
//      p.copy(child = p2.copy(projectList = p2.projectList.filter(p.references.contains)))
//    case p @ Project(_, a: Aggregate) if !a.outputSet.subsetOf(p.references) =>
//      Rule.incrementRuleCount(this.ruleName, 1)
//      p.copy(child = a.copy(aggregateExpressions = a.aggregateExpressions.filter(p.references.contains)))
//    case a @ Project(_, e @ Expand(_, _, grandChild)) if !e.outputSet.subsetOf(a.references) =>
//      Rule.incrementRuleCount(this.ruleName, 2)
//      val newOutput = e.output.filter(a.references.contains(_))
//      val newProjects = e.projections.map {
//        proj => proj.zip(e.output).filter {
//          case (_, a) =>
//            newOutput.contains(a)
//        }.unzip._1
//      }
//      a.copy(child = Expand(newProjects, newOutput, grandChild))
//    case d @ DeserializeToObject(_, _, child) if !child.outputSet.subsetOf(d.references) =>
//      Rule.incrementRuleCount(this.ruleName, 3)
//      d.copy(child = prunedChild(child, d.references))
//    case a @ Aggregate(_, _, child) if !child.outputSet.subsetOf(a.references) =>
//      Rule.incrementRuleCount(this.ruleName, 4)
//      a.copy(child = prunedChild(child, a.references))
//    case f @ FlatMapGroupsInPandas(_, _, _, child) if !child.outputSet.subsetOf(f.references) =>
//      Rule.incrementRuleCount(this.ruleName, 5)
//      f.copy(child = prunedChild(child, f.references))
//    case e @ Expand(_, _, child) if !child.outputSet.subsetOf(e.references) =>
//      Rule.incrementRuleCount(this.ruleName, 6)
//      e.copy(child = prunedChild(child, e.references))
//    case s @ ScriptTransformation(_, _, _, child, _) if !child.outputSet.subsetOf(s.references) =>
//      Rule.incrementRuleCount(this.ruleName, 7)
//      s.copy(child = prunedChild(child, s.references))
//    case p @ Project(_, g: Generate) if p.references != g.outputSet =>
//      val requiredAttrs = p.references -- g.producedAttributes ++ g.generator.references
//      val newChild = prunedChild(g.child, requiredAttrs)
//      val unrequired = g.generator.references -- p.references
//      val unrequiredIndices = newChild.output.zipWithIndex.filter(t => unrequired.contains(t._1)).map(_._2)
//      Rule.incrementRuleCount(this.ruleName, 8)
//      p.copy(child = g.copy(child = newChild, unrequiredChildIndex = unrequiredIndices))
//    case p @ Project(projectList, g: Generate) if SQLConf.get.nestedPruningOnExpressions && NestedColumnAliasing.canPruneGenerator(g.generator) =>
//      val exprsToPrune = projectList ++ g.generator.children
//      NestedColumnAliasing.getAliasSubMap(exprsToPrune, g.qualifiedGeneratorOutput).map {
//        case (nestedFieldToAlias, attrToAliases) =>
//          val newGenerator = g.generator.transform {
//            case f: ExtractValue if nestedFieldToAlias.contains(f) =>
//              nestedFieldToAlias(f).toAttribute
//          }.asInstanceOf[Generator]
//          val newGenerate = g.copy(generator = newGenerator)
//          val newChild = NestedColumnAliasing.replaceChildrenWithAliases(newGenerate, attrToAliases)
//          Rule.incrementRuleCount(this.ruleName, 9)
//          Project(NestedColumnAliasing.getNewProjectList(projectList, nestedFieldToAlias), newChild)
//      }.getOrElse(p)
//    case j @ Join(_, right, LeftExistence(_), _, _) =>
//      Rule.incrementRuleCount(this.ruleName, 10)
//      j.copy(right = prunedChild(right, j.references))
//    case p @ Project(_, _: SetOperation) => p
//    case p @ Project(_, _: Distinct) => p
//    case p @ Project(_, u: Union) =>
//      if (!u.outputSet.subsetOf(p.references)) {
//        println("Hello")
//        val firstChild = u.children.head
//        val newOutput = prunedChild(firstChild, p.references).output
//        val newChildren = u.children.map { p =>
//          val selected = p.output.zipWithIndex.filter {
//            case (a, i) =>
//              newOutput.contains(firstChild.output(i))
//          }.map(_._1)
//          Project(selected, p)
//        }
//        Rule.incrementRuleCount(this.ruleName, 11)
//        p.copy(child = u.withNewChildren(newChildren))
//      } else {
//        println("Hello")
//        p
//      }
//    case p @ Project(_, w: Window) if !w.windowOutputSet.subsetOf(p.references) =>
//      Rule.incrementRuleCount(this.ruleName, 12)
//      p.copy(child = w.copy(windowExpressions = w.windowExpressions.filter(p.references.contains)))
//    case p @ Project(_, _: LeafNode) => p
//    case p @ NestedColumnAliasing(nestedFieldToAlias, attrToAliases) =>
//      Rule.incrementRuleCount(this.ruleName, 13)
//      NestedColumnAliasing.replaceToAliases(p, nestedFieldToAlias, attrToAliases)
//    case p @ Project(_, child) if !child.isInstanceOf[Project] =>
//      val required = child.references ++ p.references
//      if (!child.inputSet.subsetOf(required)) {
//        println("Hello")
//        val newChildren = child.children.map(c => prunedChild(c, required))
//        Rule.incrementRuleCount(this.ruleName, 14)
//        p.copy(child = child.withNewChildren(newChildren))
//      } else {
//        println("Hello")
//        p
//      }
//  })
//  private def prunedChild(c: LogicalPlan, allReferences: AttributeSet) = if (!c.outputSet.subsetOf(allReferences)) {
//    println("Hello")
//    Project(c.output.filter(allReferences.contains), c)
//  } else {
//    println("Hello")
//    c
//  }
//  private def removeProjectBeforeFilter(plan: LogicalPlan): LogicalPlan = plan transformUp {
//    case p1 @ Project(_, f @ Filter(_, p2 @ Project(_, child))) if p2.outputSet.subsetOf(child.outputSet) && p2.projectList.forall(_.isInstanceOf[AttributeReference]) =>
//      p1.copy(child = f.copy(child = child))
//  }
//}
//object CollapseProject extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
//    case p1 @ Project(_, p2: Project) =>
//      if (haveCommonNonDeterministicOutput(p1.projectList, p2.projectList)) {
//        println("Hello")
//        p1
//      } else {
//        println("Hello")
//        Rule.incrementRuleCount(this.ruleName, 0)
//        p2.copy(projectList = buildCleanedProjectList(p1.projectList, p2.projectList))
//      }
//    case p @ Project(_, agg: Aggregate) =>
//      if (haveCommonNonDeterministicOutput(p.projectList, agg.aggregateExpressions)) {
//        println("Hello")
//        p
//      } else {
//        println("Hello")
//        Rule.incrementRuleCount(this.ruleName, 1)
//        agg.copy(aggregateExpressions = buildCleanedProjectList(p.projectList, agg.aggregateExpressions))
//      }
//    case Project(l1, g @ GlobalLimit(_, limit @ LocalLimit(_, p2 @ Project(l2, _)))) if isRenaming(l1, l2) =>
//      val newProjectList = buildCleanedProjectList(l1, l2)
//      Rule.incrementRuleCount(this.ruleName, 2)
//      g.copy(child = limit.copy(child = p2.copy(projectList = newProjectList)))
//    case Project(l1, limit @ LocalLimit(_, p2 @ Project(l2, _))) if isRenaming(l1, l2) =>
//      val newProjectList = buildCleanedProjectList(l1, l2)
//      Rule.incrementRuleCount(this.ruleName, 3)
//      limit.copy(child = p2.copy(projectList = newProjectList))
//    case Project(l1, r @ Repartition(_, _, p @ Project(l2, _))) if isRenaming(l1, l2) =>
//      Rule.incrementRuleCount(this.ruleName, 4)
//      r.copy(child = p.copy(projectList = buildCleanedProjectList(l1, p.projectList)))
//    case Project(l1, s @ Sample(_, _, _, _, p2 @ Project(l2, _))) if isRenaming(l1, l2) =>
//      Rule.incrementRuleCount(this.ruleName, 5)
//      s.copy(child = p2.copy(projectList = buildCleanedProjectList(l1, p2.projectList)))
//  }
//  private def collectAliases(projectList: Seq[NamedExpression]): AttributeMap[Alias] = {
//    AttributeMap(projectList.collect {
//      case a: Alias =>
//        a.toAttribute -> a
//    })
//  }
//  private def haveCommonNonDeterministicOutput(upper: Seq[NamedExpression], lower: Seq[NamedExpression]): Boolean = {
//    val aliases = collectAliases(lower)
//    upper.exists(_.collect {
//      case a: Attribute if aliases.contains(a) =>
//        aliases(a).child
//    }.exists(!_.deterministic))
//  }
//  private def buildCleanedProjectList(upper: Seq[NamedExpression], lower: Seq[NamedExpression]): Seq[NamedExpression] = {
//    val aliases = collectAliases(lower)
//    val rewrittenUpper = upper.map(_.transformUp {
//      case a: Attribute =>
//        aliases.getOrElse(a, a)
//    })
//    rewrittenUpper.map {
//      p => CleanupAliases.trimNonTopLevelAliases(p).asInstanceOf[NamedExpression]
//    }
//  }
//  private def isRenaming(list1: Seq[NamedExpression], list2: Seq[NamedExpression]): Boolean = {
//    list1.length == list2.length && list1.zip(list2).forall {
//      case (e1, e2) if e1.semanticEquals(e2) => true
//      case (Alias(a: Attribute, _), b) if a.metadata == Metadata.empty && a.name == b.name => true
//      case _ => false
//    }
//  }
//}
//object CollapseRepartition extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
//    case r @ Repartition(_, _, child: RepartitionOperation) =>
//      (r.shuffle, child.shuffle) match {
//        case (false, true) =>
//          println("Hello")
//          if (r.numPartitions >= child.numPartitions) {
//            println("Hello")
//            Rule.incrementRuleCount(this.ruleName, 0)
//            child
//          } else {
//            println("Hello")
//            r
//          }
//        case _ =>
//          println("Hello")
//          Rule.incrementRuleCount(this.ruleName, 1)
//          r.copy(child = child.child)
//      }
//    case r @ RepartitionByExpression(_, child: RepartitionOperation, _) =>
//      Rule.incrementRuleCount(this.ruleName, 3)
//      r.copy(child = child.child)
//  }
//}
//object CollapseWindow extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
//    case w1 @ Window(we1, ps1, os1, w2 @ Window(we2, ps2, os2, grandChild)) if ps1 == ps2 && os1 == os2 && w1.references.intersect(w2.windowOutputSet).isEmpty && we1.nonEmpty && we2.nonEmpty && WindowFunctionType.functionType(we1.head) == WindowFunctionType.functionType(we2.head) =>
//      Rule.incrementRuleCount(this.ruleName, 0)
//      w1.copy(windowExpressions = we2 ++ we1, child = grandChild)
//  }
//}
//object TransposeWindow extends Rule[LogicalPlan] {
//  private def compatibleParititions(ps1: Seq[Expression], ps2: Seq[Expression]): Boolean = {
//    ps1.length < ps2.length && ps2.take(ps1.length).permutations.exists(ps1.zip(_).forall {
//      case (l, r) =>
//        l.semanticEquals(r)
//    })
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
//    case w1 @ Window(we1, ps1, os1, w2 @ Window(we2, ps2, os2, grandChild)) if w1.references.intersect(w2.windowOutputSet).isEmpty && w1.expressions.forall(_.deterministic) && w2.expressions.forall(_.deterministic) && compatibleParititions(ps1, ps2) =>
//      Rule.incrementRuleCount(this.ruleName, 0)
//      Project(w1.output, Window(we2, ps2, os2, Window(we1, ps1, os1, grandChild)))
//  }
//}
//object InferFiltersFromConstraints extends Rule[LogicalPlan] with PredicateHelper with ConstraintHelper {
//  def apply(plan: LogicalPlan): LogicalPlan = {
//    if (SQLConf.get.constraintPropagationEnabled) {
//      println("Hello")
//      Rule.incrementRuleCount(this.ruleName, 0)
//      inferFilters(plan)
//    } else {
//      println("Hello")
//      plan
//    }
//  }
//  private def inferFilters(plan: LogicalPlan): LogicalPlan = plan transform {
//    case filter @ Filter(condition, child) =>
//      val newFilters = filter.constraints -- (child.constraints ++ splitConjunctivePredicates(condition))
//      if (newFilters.nonEmpty) {
//        println("Hello")
//        Filter(And(newFilters.reduce(And), condition), child)
//      } else {
//        println("Hello")
//        filter
//      }
//    case join @ Join(left, right, joinType, conditionOpt, _) =>
//      joinType match {
//        case _: InnerLike | LeftSemi =>
//          println("Hello")
//          val allConstraints = getAllConstraints(left, right, conditionOpt)
//          val newLeft = inferNewFilter(left, allConstraints)
//          val newRight = inferNewFilter(right, allConstraints)
//          join.copy(left = newLeft, right = newRight)
//        case RightOuter =>
//          println("Hello")
//          val allConstraints = getAllConstraints(left, right, conditionOpt)
//          val newLeft = inferNewFilter(left, allConstraints)
//          join.copy(left = newLeft)
//        case LeftOuter | LeftAnti =>
//          println("Hello")
//          val allConstraints = getAllConstraints(left, right, conditionOpt)
//          val newRight = inferNewFilter(right, allConstraints)
//          join.copy(right = newRight)
//        case _ =>
//          println("Hello")
//          join
//      }
//  }
//  private def getAllConstraints(left: LogicalPlan, right: LogicalPlan, conditionOpt: Option[Expression]): Set[Expression] = {
//    val baseConstraints = left.constraints.union(right.constraints).union(conditionOpt.map(splitConjunctivePredicates).getOrElse(Nil).toSet)
//    baseConstraints.union(inferAdditionalConstraints(baseConstraints))
//  }
//  private def inferNewFilter(plan: LogicalPlan, constraints: Set[Expression]): LogicalPlan = {
//    val newPredicates = constraints.union(constructIsNotNullConstraints(constraints, plan.output)).filter {
//      c => c.references.nonEmpty && c.references.subsetOf(plan.outputSet) && c.deterministic
//    } -- plan.constraints
//    if (newPredicates.isEmpty) {
//      println("Hello")
//      plan
//    } else {
//      println("Hello")
//      Filter(newPredicates.reduce(And), plan)
//    }
//  }
//}
//object CombineUnions extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
//    case u: Union =>
//      Rule.incrementRuleCount(this.ruleName, 0)
//      flattenUnion(u, false)
//    case Distinct(u: Union) =>
//      Rule.incrementRuleCount(this.ruleName, 1)
//      Distinct(flattenUnion(u, true))
//  }
//  private def flattenUnion(union: Union, flattenDistinct: Boolean): Union = {
//    val stack = mutable.Stack[LogicalPlan](union)
//    val flattened = mutable.ArrayBuffer.empty[LogicalPlan]
//    while (stack.nonEmpty) {
//      stack.pop() match {
//        case Distinct(Union(children)) if flattenDistinct =>
//          println("Hello")
//          stack.pushAll(children.reverse)
//        case Union(children) =>
//          println("Hello")
//          stack.pushAll(children.reverse)
//        case child =>
//          println("Hello")
//          flattened += child
//      }
//    }
//    Union(flattened)
//  }
//}
//object CombineFilters extends Rule[LogicalPlan] with PredicateHelper {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform applyLocally
//  val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
//    case Filter(fc, nf @ Filter(nc, grandChild)) if fc.deterministic && nc.deterministic =>
//      (ExpressionSet(splitConjunctivePredicates(fc)) -- ExpressionSet(splitConjunctivePredicates(nc))).reduceOption(And) match {
//        case Some(ac) =>
//          println("Hello")
//          Rule.incrementRuleCount(this.ruleName, 0)
//          Filter(And(nc, ac), grandChild)
//        case None =>
//          println("Hello")
//          nf
//      }
//  }
//}
//object EliminateSorts extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case s @ Sort(orders, _, child) if orders.isEmpty || orders.exists(_.child.foldable) =>
//      val newOrders = orders.filterNot(_.child.foldable)
//      Rule.incrementRuleCount(this.ruleName, 0)
//      if (newOrders.isEmpty) {
//        println("Hello")
//        child
//      } else {
//        println("Hello")
//        s.copy(order = newOrders)
//      }
//    case Sort(orders, true, child) if SortOrder.orderingSatisfies(child.outputOrdering, orders) =>
//      Rule.incrementRuleCount(this.ruleName, 1)
//      child
//    case s @ Sort(_, _, child) =>
//      Rule.incrementRuleCount(this.ruleName, 2)
//      s.copy(child = recursiveRemoveSort(child))
//    case j @ Join(originLeft, originRight, _, cond, _) if cond.forall(_.deterministic) =>
//      Rule.incrementRuleCount(this.ruleName, 3)
//      j.copy(left = recursiveRemoveSort(originLeft), right = recursiveRemoveSort(originRight))
//    case g @ Aggregate(_, aggs, originChild) if isOrderIrrelevantAggs(aggs) =>
//      Rule.incrementRuleCount(this.ruleName, 4)
//      g.copy(child = recursiveRemoveSort(originChild))
//  }
//  private def recursiveRemoveSort(plan: LogicalPlan): LogicalPlan = plan match {
//    case Sort(_, _, child) =>
//      println("Hello")
//      recursiveRemoveSort(child)
//    case other if canEliminateSort(other) =>
//      println("Hello")
//      other.withNewChildren(other.children.map(recursiveRemoveSort))
//    case _ =>
//      println("Hello")
//      plan
//  }
//  private def canEliminateSort(plan: LogicalPlan): Boolean = plan match {
//    case p: Project =>
//      println("Hello")
//      p.projectList.forall(_.deterministic)
//    case f: Filter =>
//      println("Hello")
//      f.condition.deterministic
//    case _ =>
//      println("Hello")
//      false
//  }
//  private def isOrderIrrelevantAggs(aggs: Seq[NamedExpression]): Boolean = {
//    def isOrderIrrelevantAggFunction(func: AggregateFunction): Boolean = func match {
//      case _: Min | _: Max | _: Count =>
//        println("Hello")
//        true
//      case _: Sum | _: Average | _: CentralMomentAgg =>
//        println("Hello")
//        !Seq(FloatType, DoubleType).exists(_.sameType(func.children.head.dataType))
//      case _ =>
//        println("Hello")
//        false
//    }
//    def checkValidAggregateExpression(expr: Expression): Boolean = expr match {
//      case _: AttributeReference =>
//        println("Hello")
//        true
//      case ae: AggregateExpression =>
//        println("Hello")
//        isOrderIrrelevantAggFunction(ae.aggregateFunction)
//      case _: UserDefinedExpression =>
//        println("Hello")
//        false
//      case e =>
//        println("Hello")
//        e.children.forall(checkValidAggregateExpression)
//    }
//    aggs.forall(checkValidAggregateExpression)
//  }
//}
//object PruneFilters extends Rule[LogicalPlan] with PredicateHelper {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case Filter(Literal(true, BooleanType), child) =>
//      Rule.incrementRuleCount(this.ruleName, 0)
//      child
//    case Filter(Literal(null, _), child) =>
//      Rule.incrementRuleCount(this.ruleName, 1)
//      LocalRelation(child.output, data = Seq.empty, isStreaming = plan.isStreaming)
//    case Filter(Literal(false, BooleanType), child) =>
//      Rule.incrementRuleCount(this.ruleName, 2)
//      LocalRelation(child.output, data = Seq.empty, isStreaming = plan.isStreaming)
//    case f @ Filter(fc, p: LogicalPlan) =>
//      val (prunedPredicates, remainingPredicates) = splitConjunctivePredicates(fc).partition {
//        cond => cond.deterministic && p.constraints.contains(cond)
//      }
//      if (prunedPredicates.isEmpty) {
//        println("Hello")
//        f
//      } else if (remainingPredicates.isEmpty) {
//        println("Hello")
//        println("Hello")
//        p
//      } else {
//        println("Hello")
//        println("Hello")
//        val newCond = remainingPredicates.reduce(And)
//        Rule.incrementRuleCount(this.ruleName, 3)
//        Filter(newCond, p)
//      }
//  }
//}
//object PushDownPredicates extends Rule[LogicalPlan] with PredicateHelper {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    Rule.incrementRuleCount(this.ruleName, 0)
//    CombineFilters.applyLocally.orElse(PushPredicateThroughNonJoin.applyLocally).orElse(PushPredicateThroughJoin.applyLocally)
//  }
//}
//object PushPredicateThroughNonJoin extends Rule[LogicalPlan] with PredicateHelper {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform applyLocally
//  val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
//    case Filter(condition, project @ Project(fields, grandChild)) if fields.forall(_.deterministic) && canPushThroughCondition(grandChild, condition) =>
//      val aliasMap = getAliasMap(project)
//      Rule.incrementRuleCount(this.ruleName, 0)
//      project.copy(child = Filter(replaceAlias(condition, aliasMap), grandChild))
//    case filter @ Filter(condition, aggregate: Aggregate) if aggregate.aggregateExpressions.forall(_.deterministic) && aggregate.groupingExpressions.nonEmpty =>
//      val aliasMap = getAliasMap(aggregate)
//      val (candidates, nonDeterministic) = splitConjunctivePredicates(condition).partition(_.deterministic)
//      val (pushDown, rest) = candidates.partition { cond =>
//        val replaced = replaceAlias(cond, aliasMap)
//        cond.references.nonEmpty && replaced.references.subsetOf(aggregate.child.outputSet)
//      }
//      val stayUp = rest ++ nonDeterministic
//      if (pushDown.nonEmpty) {
//        println("Hello")
//        val pushDownPredicate = pushDown.reduce(And)
//        val replaced = replaceAlias(pushDownPredicate, aliasMap)
//        val newAggregate = aggregate.copy(child = Filter(replaced, aggregate.child))
//        Rule.incrementRuleCount(this.ruleName, 1)
//        if (stayUp.isEmpty) {
//          println("Hello")
//          newAggregate
//        } else {
//          println("Hello")
//          Filter(stayUp.reduce(And), newAggregate)
//        }
//      } else {
//        println("Hello")
//        filter
//      }
//    case filter @ Filter(condition, w: Window) if w.partitionSpec.forall(_.isInstanceOf[AttributeReference]) =>
//      val partitionAttrs = AttributeSet(w.partitionSpec.flatMap(_.references))
//      val (candidates, nonDeterministic) = splitConjunctivePredicates(condition).partition(_.deterministic)
//      val (pushDown, rest) = candidates.partition {
//        cond => cond.references.subsetOf(partitionAttrs)
//      }
//      val stayUp = rest ++ nonDeterministic
//      if (pushDown.nonEmpty) {
//        println("Hello")
//        val pushDownPredicate = pushDown.reduce(And)
//        val newWindow = w.copy(child = Filter(pushDownPredicate, w.child))
//        if (stayUp.isEmpty) {
//          println("Hello")
//          newWindow
//        } else {
//          println("Hello")
//          Filter(stayUp.reduce(And), newWindow)
//        }
//      } else {
//        println("Hello")
//        filter
//      }
//    case filter @ Filter(condition, union: Union) =>
//      val (pushDown, stayUp) = splitConjunctivePredicates(condition).partition(_.deterministic)
//      if (pushDown.nonEmpty) {
//        println("Hello")
//        val pushDownCond = pushDown.reduceLeft(And)
//        val output = union.output
//        val newGrandChildren = union.children.map { grandchild =>
//          val newCond = pushDownCond transform {
//            case e if output.exists(_.semanticEquals(e)) =>
//              grandchild.output(output.indexWhere(_.semanticEquals(e)))
//          }
//          assert(newCond.references.subsetOf(grandchild.outputSet))
//          Filter(newCond, grandchild)
//        }
//        val newUnion = union.withNewChildren(newGrandChildren)
//        if (stayUp.nonEmpty) {
//          println("Hello")
//          Filter(stayUp.reduceLeft(And), newUnion)
//        } else {
//          println("Hello")
//          newUnion
//        }
//      } else {
//        println("Hello")
//        filter
//      }
//    case filter @ Filter(condition, watermark: EventTimeWatermark) =>
//      val (pushDown, stayUp) = splitConjunctivePredicates(condition).partition {
//        p => p.deterministic && !p.references.contains(watermark.eventTime)
//      }
//      if (pushDown.nonEmpty) {
//        println("Hello")
//        val pushDownPredicate = pushDown.reduceLeft(And)
//        val newWatermark = watermark.copy(child = Filter(pushDownPredicate, watermark.child))
//        if (stayUp.isEmpty) {
//          println("Hello")
//          newWatermark
//        } else {
//          println("Hello")
//          Filter(stayUp.reduceLeft(And), newWatermark)
//        }
//      } else {
//        println("Hello")
//        filter
//      }
//    case filter @ Filter(_, u: UnaryNode) if canPushThrough(u) && u.expressions.forall(_.deterministic) =>
//      pushDownPredicate(filter, u.child) {
//        predicate => u.withNewChildren(Seq(Filter(predicate, u.child)))
//      }
//  }
//  def getAliasMap(plan: Project): AttributeMap[Expression] = {
//    AttributeMap(plan.projectList.collect {
//      case a: Alias =>
//        (a.toAttribute, a.child)
//    })
//  }
//  def getAliasMap(plan: Aggregate): AttributeMap[Expression] = {
//    val aliasMap = plan.aggregateExpressions.collect {
//      case a: Alias if a.child.find(e => e.isInstanceOf[AggregateExpression] || PythonUDF.isGroupedAggPandasUDF(e)).isEmpty =>
//        (a.toAttribute, a.child)
//    }
//    AttributeMap(aliasMap)
//  }
//  def canPushThrough(p: UnaryNode): Boolean = p match {
//    case _: AppendColumns =>
//      println("Hello")
//      true
//    case _: Distinct =>
//      println("Hello")
//      true
//    case _: Generate =>
//      println("Hello")
//      true
//    case _: Pivot =>
//      println("Hello")
//      true
//    case _: RepartitionByExpression =>
//      println("Hello")
//      true
//    case _: Repartition =>
//      println("Hello")
//      true
//    case _: ScriptTransformation =>
//      println("Hello")
//      true
//    case _: Sort =>
//      println("Hello")
//      true
//    case _: BatchEvalPython =>
//      println("Hello")
//      true
//    case _: ArrowEvalPython =>
//      println("Hello")
//      true
//    case _ =>
//      println("Hello")
//      false
//  }
//  private def pushDownPredicate(filter: Filter, grandchild: LogicalPlan)(insertFilter: Expression => LogicalPlan): LogicalPlan = {
//    val (candidates, nonDeterministic) = splitConjunctivePredicates(filter.condition).partition(_.deterministic)
//    val (pushDown, rest) = candidates.partition {
//      cond => cond.references.subsetOf(grandchild.outputSet)
//    }
//    val stayUp = rest ++ nonDeterministic
//    if (pushDown.nonEmpty) {
//      println("Hello")
//      val newChild = insertFilter(pushDown.reduceLeft(And))
//      if (stayUp.nonEmpty) {
//        println("Hello")
//        Filter(stayUp.reduceLeft(And), newChild)
//      } else {
//        println("Hello")
//        newChild
//      }
//    } else {
//      println("Hello")
//      filter
//    }
//  }
//  private def canPushThroughCondition(plan: LogicalPlan, condition: Expression): Boolean = {
//    val attributes = plan.outputSet
//    val matched = condition.find {
//      case s: SubqueryExpression =>
//        s.plan.outputSet.intersect(attributes).nonEmpty
//      case _ => false
//    }
//    matched.isEmpty
//  }
//}
//object PushPredicateThroughJoin extends Rule[LogicalPlan] with PredicateHelper {
//  private def split(condition: Seq[Expression], left: LogicalPlan, right: LogicalPlan) = {
//    val (pushDownCandidates, nonDeterministic) = condition.partition(_.deterministic)
//    val (leftEvaluateCondition, rest) = pushDownCandidates.partition(_.references.subsetOf(left.outputSet))
//    val (rightEvaluateCondition, commonCondition) = rest.partition(expr => expr.references.subsetOf(right.outputSet))
//    (leftEvaluateCondition, rightEvaluateCondition, commonCondition ++ nonDeterministic)
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform applyLocally
//  val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
//    case f @ Filter(filterCondition, Join(left, right, joinType, joinCondition, hint)) =>
//      val (leftFilterConditions, rightFilterConditions, commonFilterCondition) = split(splitConjunctivePredicates(filterCondition), left, right)
//      joinType match {
//        case _: InnerLike =>
//          println("Hello")
//          val newLeft = leftFilterConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
//          val newRight = rightFilterConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
//          val (newJoinConditions, others) = commonFilterCondition.partition(canEvaluateWithinJoin)
//          val newJoinCond = (newJoinConditions ++ joinCondition).reduceLeftOption(And)
//          val join = Join(newLeft, newRight, joinType, newJoinCond, hint)
//          Rule.incrementRuleCount(this.ruleName, 0)
//          if (others.nonEmpty) {
//            println("Hello")
//            Filter(others.reduceLeft(And), join)
//          } else {
//            println("Hello")
//            join
//          }
//        case RightOuter =>
//          println("Hello")
//          val newLeft = left
//          val newRight = rightFilterConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
//          val newJoinCond = joinCondition
//          val newJoin = Join(newLeft, newRight, RightOuter, newJoinCond, hint)
//          Rule.incrementRuleCount(this.ruleName, 1)
//          (leftFilterConditions ++ commonFilterCondition).reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
//        case LeftOuter | LeftExistence(_) =>
//          println("Hello")
//          val newLeft = leftFilterConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
//          val newRight = right
//          val newJoinCond = joinCondition
//          val newJoin = Join(newLeft, newRight, joinType, newJoinCond, hint)
//          Rule.incrementRuleCount(this.ruleName, 2)
//          (rightFilterConditions ++ commonFilterCondition).reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
//        case FullOuter =>
//          println("Hello")
//          f
//        case NaturalJoin(_) =>
//          println("Hello")
//          sys.error("Untransformed NaturalJoin node")
//        case UsingJoin(_, _) =>
//          println("Hello")
//          sys.error("Untransformed Using join node")
//      }
//    case j @ Join(left, right, joinType, joinCondition, hint) =>
//      val (leftJoinConditions, rightJoinConditions, commonJoinCondition) = split(joinCondition.map(splitConjunctivePredicates).getOrElse(Nil), left, right)
//      joinType match {
//        case _: InnerLike | LeftSemi =>
//          println("Hello")
//          val newLeft = leftJoinConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
//          val newRight = rightJoinConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
//          val newJoinCond = commonJoinCondition.reduceLeftOption(And)
//          Rule.incrementRuleCount(this.ruleName, 3)
//          Join(newLeft, newRight, joinType, newJoinCond, hint)
//        case RightOuter =>
//          println("Hello")
//          val newLeft = leftJoinConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
//          val newRight = right
//          val newJoinCond = (rightJoinConditions ++ commonJoinCondition).reduceLeftOption(And)
//          Rule.incrementRuleCount(this.ruleName, 4)
//          Join(newLeft, newRight, RightOuter, newJoinCond, hint)
//        case LeftOuter | LeftAnti | ExistenceJoin(_) =>
//          println("Hello")
//          val newLeft = left
//          val newRight = rightJoinConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
//          val newJoinCond = (leftJoinConditions ++ commonJoinCondition).reduceLeftOption(And)
//          Rule.incrementRuleCount(this.ruleName, 5)
//          Join(newLeft, newRight, joinType, newJoinCond, hint)
//        case FullOuter =>
//          println("Hello")
//          j
//        case NaturalJoin(_) =>
//          println("Hello")
//          sys.error("Untransformed NaturalJoin node")
//        case UsingJoin(_, _) =>
//          println("Hello")
//          sys.error("Untransformed Using join node")
//      }
//  }
//}
//object CombineLimits extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case GlobalLimit(le, GlobalLimit(ne, grandChild)) =>
//      Rule.incrementRuleCount(this.ruleName, 0)
//      GlobalLimit(Least(Seq(ne, le)), grandChild)
//    case LocalLimit(le, LocalLimit(ne, grandChild)) =>
//      Rule.incrementRuleCount(this.ruleName, 1)
//      LocalLimit(Least(Seq(ne, le)), grandChild)
//    case Limit(le, Limit(ne, grandChild)) =>
//      Rule.incrementRuleCount(this.ruleName, 2)
//      Limit(Least(Seq(ne, le)), grandChild)
//  }
//}
//object CheckCartesianProducts extends Rule[LogicalPlan] with PredicateHelper {
//  def isCartesianProduct(join: Join): Boolean = {
//    val conditions = join.condition.map(splitConjunctivePredicates).getOrElse(Nil)
//    conditions match {
//      case Seq(Literal.FalseLiteral) | Seq(Literal(null, BooleanType)) =>
//        println("Hello")
//        false
//      case _ =>
//        println("Hello")
//        !conditions.map(_.references).exists(refs => refs.exists(join.left.outputSet.contains) && refs.exists(join.right.outputSet.contains))
//    }
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = if (SQLConf.get.crossJoinEnabled) {
//    println("Hello")
//    plan
//  } else {
//    println("Hello")
//    plan transform {
//      case j @ Join(left, right, Inner | LeftOuter | RightOuter | FullOuter, _, _) if isCartesianProduct(j) =>
//        Rule.incrementRuleCount(this.ruleName, 0)
//        throw new AnalysisException(s"""Detected implicit cartesian product for ${
//          j.joinType.sql
//        } join between logical plans
//               |${
//          left.treeString(false).trim
//        }
//               |and
//               |${
//          right.treeString(false).trim
//        }
//               |Join condition is missing or trivial.
//               |Either: use the CROSS JOIN syntax to allow cartesian products between these
//               |relations, or: enable implicit cartesian products by setting the configuration
//               |variable spark.sql.crossJoin.enabled=true""".stripMargin)
//    }
//  }
//}
//object DecimalAggregates extends Rule[LogicalPlan] {
//  import Decimal.MAX_LONG_DIGITS
//  private val MAX_DOUBLE_DIGITS = 15
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case q: LogicalPlan =>
//      q transformExpressionsDown {
//        case we @ WindowExpression(ae @ AggregateExpression(af, _, _, _, _), _) =>
//          af match {
//            case Sum(e @ DecimalType.Expression(prec, scale)) if prec + 10 <= MAX_LONG_DIGITS =>
//              println("Hello")
//              Rule.incrementRuleCount(this.ruleName, 0)
//              MakeDecimal(we.copy(windowFunction = ae.copy(aggregateFunction = Sum(UnscaledValue(e)))), prec + 10, scale)
//            case Average(e @ DecimalType.Expression(prec, scale)) if prec + 4 <= MAX_DOUBLE_DIGITS =>
//              println("Hello")
//              Rule.incrementRuleCount(this.ruleName, 1)
//              val newAggExpr = we.copy(windowFunction = ae.copy(aggregateFunction = Average(UnscaledValue(e))))
//              Cast(Divide(newAggExpr, Literal.create(math.pow(10.0d, scale), DoubleType)), DecimalType(prec + 4, scale + 4), Option(SQLConf.get.sessionLocalTimeZone))
//            case _ =>
//              println("Hello")
//              we
//          }
//        case ae @ AggregateExpression(af, _, _, _, _) =>
//          af match {
//            case Sum(e @ DecimalType.Expression(prec, scale)) if prec + 10 <= MAX_LONG_DIGITS =>
//              println("Hello")
//              Rule.incrementRuleCount(this.ruleName, 2)
//              MakeDecimal(ae.copy(aggregateFunction = Sum(UnscaledValue(e))), prec + 10, scale)
//            case Average(e @ DecimalType.Expression(prec, scale)) if prec + 4 <= MAX_DOUBLE_DIGITS =>
//              println("Hello")
//              Rule.incrementRuleCount(this.ruleName, 3)
//              val newAggExpr = ae.copy(aggregateFunction = Average(UnscaledValue(e)))
//              Cast(Divide(newAggExpr, Literal.create(math.pow(10.0d, scale), DoubleType)), DecimalType(prec + 4, scale + 4), Option(SQLConf.get.sessionLocalTimeZone))
//            case _ =>
//              println("Hello")
//              ae
//          }
//      }
//  }
//}
//object ConvertToLocalRelation extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case Project(projectList, LocalRelation(output, data, isStreaming)) if !projectList.exists(hasUnevaluableExpr) =>
//      val projection = new InterpretedMutableProjection(projectList, output)
//      projection.initialize(0)
//      Rule.incrementRuleCount(this.ruleName, 0)
//      LocalRelation(projectList.map(_.toAttribute), data.map(projection(_).copy()), isStreaming)
//    case Limit(IntegerLiteral(limit), LocalRelation(output, data, isStreaming)) =>
//      Rule.incrementRuleCount(this.ruleName, 1)
//      LocalRelation(output, data.take(limit), isStreaming)
//    case Filter(condition, LocalRelation(output, data, isStreaming)) if !hasUnevaluableExpr(condition) =>
//      val predicate = Predicate.create(condition, output)
//      predicate.initialize(0)
//      Rule.incrementRuleCount(this.ruleName, 2)
//      LocalRelation(output, data.filter(row => predicate.eval(row)), isStreaming)
//  }
//  private def hasUnevaluableExpr(expr: Expression): Boolean = {
//    expr.find(e => e.isInstanceOf[Unevaluable] && !e.isInstanceOf[AttributeReference]).isDefined
//  }
//}
//object ReplaceDistinctWithAggregate extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case Distinct(child) =>
//      Rule.incrementRuleCount(this.ruleName, 0)
//      Aggregate(child.output, child.output, child)
//  }
//}
//object ReplaceDeduplicateWithAggregate extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case Deduplicate(keys, child) if !child.isStreaming =>
//      val keyExprIds = keys.map(_.exprId)
//      val aggCols = child.output.map {
//        attr => if (keyExprIds.contains(attr.exprId)) {
//          println("Hello")
//          attr
//        } else {
//          println("Hello")
//          Rule.incrementRuleCount(this.ruleName, 0)
//          Alias(new First(attr).toAggregateExpression(), attr.name)(attr.exprId)
//        }
//      }
//      val nonemptyKeys = if (keys.isEmpty) {
//        println("Hello")
//        Literal(1) :: Nil
//      } else {
//        println("Hello")
//        keys
//      }
//      Aggregate(nonemptyKeys, aggCols, child)
//  }
//}
//object ReplaceIntersectWithSemiJoin extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case Intersect(left, right, false) =>
//      assert(left.output.size == right.output.size)
//      val joinCond = left.output.zip(right.output).map {
//        case (l, r) =>
//          EqualNullSafe(l, r)
//      }
//      Rule.incrementRuleCount(this.ruleName, 0)
//      Distinct(Join(left, right, LeftSemi, joinCond.reduceLeftOption(And), JoinHint.NONE))
//  }
//}
//object ReplaceExceptWithAntiJoin extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case Except(left, right, false) =>
//      assert(left.output.size == right.output.size)
//      val joinCond = left.output.zip(right.output).map {
//        case (l, r) =>
//          EqualNullSafe(l, r)
//      }
//      Rule.incrementRuleCount(this.ruleName, 0)
//      Distinct(Join(left, right, LeftAnti, joinCond.reduceLeftOption(And), JoinHint.NONE))
//  }
//}
//object RewriteExceptAll extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case Except(left, right, true) =>
//      assert(left.output.size == right.output.size)
//      val newColumnLeft = Alias(Literal(1L), "vcol")()
//      val newColumnRight = Alias(Literal(-1L), "vcol")()
//      val modifiedLeftPlan = Project(Seq(newColumnLeft) ++ left.output, left)
//      val modifiedRightPlan = Project(Seq(newColumnRight) ++ right.output, right)
//      val unionPlan = Union(modifiedLeftPlan, modifiedRightPlan)
//      val aggSumCol = Alias(AggregateExpression(Sum(unionPlan.output.head.toAttribute), Complete, false), "sum")()
//      val aggOutputColumns = left.output ++ Seq(aggSumCol)
//      val aggregatePlan = Aggregate(left.output, aggOutputColumns, unionPlan)
//      val filteredAggPlan = Filter(GreaterThan(aggSumCol.toAttribute, Literal(0L)), aggregatePlan)
//      val genRowPlan = Generate(ReplicateRows(Seq(aggSumCol.toAttribute) ++ left.output), unrequiredChildIndex = Nil, outer = false, qualifier = None, left.output, filteredAggPlan)
//      Rule.incrementRuleCount(this.ruleName, 0)
//      Project(left.output, genRowPlan)
//  }
//}
//object RewriteIntersectAll extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case Intersect(left, right, true) =>
//      assert(left.output.size == right.output.size)
//      val trueVcol1 = Alias(Literal(true), "vcol1")()
//      val nullVcol1 = Alias(Literal(null, BooleanType), "vcol1")()
//      val trueVcol2 = Alias(Literal(true), "vcol2")()
//      val nullVcol2 = Alias(Literal(null, BooleanType), "vcol2")()
//      val leftPlanWithAddedVirtualCols = Project(Seq(trueVcol1, nullVcol2) ++ left.output, left)
//      val rightPlanWithAddedVirtualCols = Project(Seq(nullVcol1, trueVcol2) ++ right.output, right)
//      val unionPlan = Union(leftPlanWithAddedVirtualCols, rightPlanWithAddedVirtualCols)
//      val vCol1AggrExpr = Alias(AggregateExpression(Count(unionPlan.output(0)), Complete, false), "vcol1_count")()
//      val vCol2AggrExpr = Alias(AggregateExpression(Count(unionPlan.output(1)), Complete, false), "vcol2_count")()
//      val ifExpression = Alias(If(GreaterThan(vCol1AggrExpr.toAttribute, vCol2AggrExpr.toAttribute), vCol2AggrExpr.toAttribute, vCol1AggrExpr.toAttribute), "min_count")()
//      val aggregatePlan = Aggregate(left.output, Seq(vCol1AggrExpr, vCol2AggrExpr) ++ left.output, unionPlan)
//      val filterPlan = Filter(And(GreaterThanOrEqual(vCol1AggrExpr.toAttribute, Literal(1L)), GreaterThanOrEqual(vCol2AggrExpr.toAttribute, Literal(1L))), aggregatePlan)
//      val projectMinPlan = Project(left.output ++ Seq(ifExpression), filterPlan)
//      val genRowPlan = Generate(ReplicateRows(Seq(ifExpression.toAttribute) ++ left.output), unrequiredChildIndex = Nil, outer = false, qualifier = None, left.output, projectMinPlan)
//      Rule.incrementRuleCount(this.ruleName, 0)
//      Project(left.output, genRowPlan)
//  }
//}
//object RemoveLiteralFromGroupExpressions extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case a @ Aggregate(grouping, _, _) if grouping.nonEmpty =>
//      val newGrouping = grouping.filter(!_.foldable)
//      if (newGrouping.nonEmpty) {
//        println("Hello")
//        Rule.incrementRuleCount(this.ruleName, 0)
//        a.copy(groupingExpressions = newGrouping)
//      } else {
//        println("Hello")
//        Rule.incrementRuleCount(this.ruleName, 1)
//        a.copy(groupingExpressions = Seq(Literal(0, IntegerType)))
//      }
//  }
//}
//object RemoveRepetitionFromGroupExpressions extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case a @ Aggregate(grouping, _, _) if grouping.size > 1 =>
//      val newGrouping = ExpressionSet(grouping).toSeq
//      if (newGrouping.size == grouping.size) {
//        println("Hello")
//        a
//      } else {
//        println("Hello")
//        Rule.incrementRuleCount(this.ruleName, 0)
//        a.copy(groupingExpressions = newGrouping)
//      }
//  }
//}
//object OptimizeLimitZero extends Rule[LogicalPlan] {
//  private def empty(plan: LogicalPlan) = LocalRelation(plan.output, data = Seq.empty, isStreaming = plan.isStreaming)
//  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
//    case gl @ GlobalLimit(IntegerLiteral(0), _) =>
//      Rule.incrementRuleCount(this.ruleName, 0)
//      empty(gl)
//    case ll @ LocalLimit(IntegerLiteral(0), _) =>
//      Rule.incrementRuleCount(this.ruleName, 1)
//      empty(ll)
//  }
//}