//package org.apache.spark.sql.catalyst.optimizer
//import scala.collection.mutable
//import org.apache.spark.sql.catalyst.SQLConfHelper
//import org.apache.spark.sql.catalyst.analysis._
//import org.apache.spark.sql.catalyst.catalog.{ InMemoryCatalog, SessionCatalog }
//import org.apache.spark.sql.catalyst.expressions._
//import org.apache.spark.sql.catalyst.expressions.aggregate._
//import org.apache.spark.sql.catalyst.plans._
//import org.apache.spark.sql.catalyst.plans.logical.{ RepartitionOperation, _ }
//import org.apache.spark.sql.catalyst.rules._
//import org.apache.spark.sql.catalyst.trees.AlwaysProcess
//import org.apache.spark.sql.catalyst.trees.TreePattern._
//import org.apache.spark.sql.catalyst.types.DataTypeUtils
//import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
//import org.apache.spark.sql.connector.catalog.CatalogManager
//import org.apache.spark.sql.errors.QueryCompilationErrors
//import org.apache.spark.sql.internal.SQLConf
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.util.SchemaUtils._
//import org.apache.spark.util.Utils
//abstract class Optimizer(catalogManager: CatalogManager) extends RuleExecutor[LogicalPlan] with SQLConfHelper {
//  override protected def validatePlanChanges(previousPlan: LogicalPlan, currentPlan: LogicalPlan): Option[String] = {
//    LogicalPlanIntegrity.validateOptimizedPlan(previousPlan, currentPlan)
//  }
//  override protected val excludedOnceBatches: Set[String] = Set("PartitionPruning", "RewriteSubquery", "Extract Python UDFs")
//  protected def fixedPoint = FixedPoint(conf.optimizerMaxIterations, maxIterationsSetting = SQLConf.OPTIMIZER_MAX_ITERATIONS.key)
//  def defaultBatches: Seq[Batch] = {
//    val operatorOptimizationRuleSet = Seq(PushProjectionThroughUnion, PushProjectionThroughLimit, ReorderJoin, EliminateOuterJoin, PushDownPredicates, PushDownLeftSemiAntiJoin, PushLeftSemiLeftAntiThroughJoin, LimitPushDown, LimitPushDownThroughWindow, ColumnPruning, GenerateOptimization, CollapseRepartition, CollapseProject, OptimizeWindowFunctions, CollapseWindow, EliminateOffsets, EliminateLimits, CombineUnions, OptimizeRepartition, TransposeWindow, NullPropagation, RewriteNonCorrelatedExists, NullDownPropagation, ConstantPropagation, FoldablePropagation, OptimizeIn, OptimizeRand, ConstantFolding, EliminateAggregateFilter, ReorderAssociativeOperator, LikeSimplification, BooleanSimplification, SimplifyConditionals, PushFoldableIntoBranches, RemoveDispensableExpressions, SimplifyBinaryComparison, ReplaceNullWithFalseInPredicate, PruneFilters, SimplifyCasts, SimplifyCaseConversionExpressions, RewriteCorrelatedScalarSubquery, RewriteLateralSubquery, EliminateSerialization, RemoveRedundantAliases, RemoveRedundantAggregates, UnwrapCastInBinaryComparison, RemoveNoopOperators, OptimizeUpdateFields, SimplifyExtractValueOps, OptimizeCsvJsonExprs, CombineConcats, PushdownPredicatesAndPruneColumnsForCTEDef) ++ extendedOperatorOptimizationRules
//    val operatorOptimizationBatch: Seq[Batch] = {
//      Batch("Operator Optimization before Inferring Filters", fixedPoint, operatorOptimizationRuleSet: _*) :: Batch("Infer Filters", Once, InferFiltersFromGenerate, InferFiltersFromConstraints) :: Batch("Operator Optimization after Inferring Filters", fixedPoint, operatorOptimizationRuleSet: _*) :: Batch("Push extra predicate through join", fixedPoint, PushExtraPredicateThroughJoin, PushDownPredicates) :: Nil
//    }
//    val batches = (Batch("Finish Analysis", Once, FinishAnalysis) :: Batch("Eliminate Distinct", Once, EliminateDistinct) :: Batch("Inline CTE", Once, InlineCTE()) :: Batch("Union", fixedPoint, RemoveNoopOperators, CombineUnions, RemoveNoopUnion) :: Batch("LocalRelation early", fixedPoint, ConvertToLocalRelation, PropagateEmptyRelation, UpdateAttributeNullability) :: Batch("Pullup Correlated Expressions", Once, OptimizeOneRowRelationSubquery, PullupCorrelatedPredicates) :: Batch("Subquery", FixedPoint(1), OptimizeSubqueries) :: Batch("Replace Operators", fixedPoint, RewriteExceptAll, RewriteIntersectAll, ReplaceIntersectWithSemiJoin, ReplaceExceptWithFilter, ReplaceExceptWithAntiJoin, ReplaceDistinctWithAggregate, ReplaceDeduplicateWithAggregate) :: Batch("Aggregate", fixedPoint, RemoveLiteralFromGroupExpressions, RemoveRepetitionFromGroupExpressions) :: (Nil ++ operatorOptimizationBatch)) :+ Batch("Clean Up Temporary CTE Info", Once, CleanUpTempCTEInfo) :+ Batch("Pre CBO Rules", Once, preCBORules: _*) :+ Batch("Early Filter and Projection Push-Down", Once, earlyScanPushDownRules: _*) :+ Batch("Update CTE Relation Stats", Once, UpdateCTERelationStats) :+ Batch("Join Reorder", FixedPoint(1), CostBasedJoinReorder) :+ Batch("Eliminate Sorts", Once, EliminateSorts) :+ Batch("Decimal Optimizations", fixedPoint, DecimalAggregates) :+ Batch("Distinct Aggregate Rewrite", Once, RewriteDistinctAggregates) :+ Batch("Object Expressions Optimization", fixedPoint, EliminateMapObjects, CombineTypedFilters, ObjectSerializerPruning, ReassignLambdaVariableID) :+ Batch("LocalRelation", fixedPoint, ConvertToLocalRelation, PropagateEmptyRelation, UpdateAttributeNullability) :+ Batch("Optimize One Row Plan", fixedPoint, OptimizeOneRowPlan) :+ Batch("Check Cartesian Products", Once, CheckCartesianProducts) :+ Batch("RewriteSubquery", Once, RewritePredicateSubquery, PushPredicateThroughJoin, LimitPushDown, ColumnPruning, CollapseProject, RemoveRedundantAliases, RemoveNoopOperators) :+ Batch("NormalizeFloatingNumbers", Once, NormalizeFloatingNumbers) :+ Batch("ReplaceUpdateFieldsExpression", Once, ReplaceUpdateFieldsExpression)
//    batches.filter(_.rules.nonEmpty)
//  }
//  def nonExcludableRules: Seq[String] = FinishAnalysis.ruleName :: RewriteDistinctAggregates.ruleName :: ReplaceDeduplicateWithAggregate.ruleName :: ReplaceIntersectWithSemiJoin.ruleName :: ReplaceExceptWithFilter.ruleName :: ReplaceExceptWithAntiJoin.ruleName :: RewriteExceptAll.ruleName :: RewriteIntersectAll.ruleName :: ReplaceDistinctWithAggregate.ruleName :: PullupCorrelatedPredicates.ruleName :: RewriteCorrelatedScalarSubquery.ruleName :: RewritePredicateSubquery.ruleName :: NormalizeFloatingNumbers.ruleName :: ReplaceUpdateFieldsExpression.ruleName :: RewriteLateralSubquery.ruleName :: OptimizeSubqueries.ruleName :: Nil
//  object FinishAnalysis extends Rule[LogicalPlan] {
//    private val rules = Seq(EliminateResolvedHint, EliminateSubqueryAliases, EliminateView, ReplaceExpressions, RewriteNonCorrelatedExists, PullOutGroupingExpressions, ComputeCurrentTime, ReplaceCurrentLike(catalogManager), SpecialDatetimeValues, RewriteAsOfJoin)
//    override def apply(plan: LogicalPlan): LogicalPlan = {
//      rules.foldLeft(plan) {
//        case (sp, rule) =>
//          println("FinishAnalysis.apply.case1")
//          rule.apply(sp)
//      }.transformAllExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
//        case s: SubqueryExpression =>
//          println("FinishAnalysis.apply.case2")
//          val Subquery(newPlan, _) = apply(Subquery.fromExpression(s))
//          s.withNewPlan(newPlan)
//      }
//    }
//  }
//  object OptimizeSubqueries extends Rule[LogicalPlan] {
//    private def removeTopLevelSort(plan: LogicalPlan): LogicalPlan = {
//      if (!plan.containsPattern(SORT)) {
//        println("OptimizeSubqueries.removeTopLevelSort.then1")
//        return plan
//      } else {
//        println("OptimizeSubqueries.removeTopLevelSort.else2")
//      }
//      plan match {
//        case Sort(_, _, child) =>
//          println("OptimizeSubqueries.removeTopLevelSort.match3")
//          child
//        case Project(fields, child) =>
//          println("OptimizeSubqueries.removeTopLevelSort.match4")
//          Project(fields, removeTopLevelSort(child))
//        case other =>
//          println("OptimizeSubqueries.removeTopLevelSort.match5")
//          other
//      }
//    }
//    def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION), ruleId) {
//      case d: DynamicPruningSubquery =>
//        println("OptimizeSubqueries.apply.case1")
//        d
//      case s: SubqueryExpression =>
//        println("OptimizeSubqueries.apply.case2")
//        val Subquery(newPlan, _) = Optimizer.this.execute(Subquery.fromExpression(s))
//        s.withNewPlan(removeTopLevelSort(newPlan))
//    }
//  }
//  object UpdateCTERelationStats extends Rule[LogicalPlan] {
//    override def apply(plan: LogicalPlan): LogicalPlan = {
//      if (!plan.isInstanceOf[Subquery] && plan.containsPattern(CTE)) {
//        println("UpdateCTERelationStats.apply.then1")
//        val statsMap = mutable.HashMap.empty[Long, Statistics]
//        updateCTEStats(plan, statsMap)
//      } else {
//        println("UpdateCTERelationStats.apply.else2")
//        plan
//      }
//    }
//    private def updateCTEStats(plan: LogicalPlan, statsMap: mutable.HashMap[Long, Statistics]): LogicalPlan = plan match {
//      case WithCTE(child, cteDefs) =>
//        println("UpdateCTERelationStats.updateCTEStats.match1")
//        val newDefs = cteDefs.map { cteDef =>
//          val newDef = updateCTEStats(cteDef, statsMap)
//          statsMap.put(cteDef.id, newDef.stats)
//          newDef.asInstanceOf[CTERelationDef]
//        }
//        WithCTE(updateCTEStats(child, statsMap), newDefs)
//      case c: CTERelationRef =>
//        println("UpdateCTERelationStats.updateCTEStats.match2")
//        statsMap.get(c.cteId).map(s => c.withNewStats(Some(s))).getOrElse(c)
//      case _ if plan.containsPattern(CTE) =>
//        println("UpdateCTERelationStats.updateCTEStats.match3")
//        plan.withNewChildren(plan.children.map(child => updateCTEStats(child, statsMap))).transformExpressionsWithPruning(_.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
//          case e: SubqueryExpression =>
//            println("Unknown.Unknown.case1")
//            e.withNewPlan(updateCTEStats(e.plan, statsMap))
//        }
//      case _ =>
//        println("UpdateCTERelationStats.updateCTEStats.match4")
//        plan
//    }
//  }
//  def extendedOperatorOptimizationRules: Seq[Rule[LogicalPlan]] = Nil
//  def earlyScanPushDownRules: Seq[Rule[LogicalPlan]] = Nil
//  def preCBORules: Seq[Rule[LogicalPlan]] = Nil
//  final override def batches: Seq[Batch] = {
//    val excludedRulesConf = conf.optimizerExcludedRules.toSeq.flatMap(Utils.stringToSeq)
//    val excludedRules = excludedRulesConf.filter { ruleName =>
//      val nonExcludable = nonExcludableRules.contains(ruleName)
//      if (nonExcludable) {
//        println("Optimizer.batches.then1")
//        logWarning(s"Optimization rule '${
//          ruleName
//        }' was not excluded from the optimizer " + (s"because this rule is a non-excludable rule."))
//      } else {
//        println("Optimizer.batches.else2")
//      }
//      !nonExcludable
//    }
//    if (excludedRules.isEmpty) {
//      println("Optimizer.batches.then3")
//      defaultBatches
//    } else {
//      println("Optimizer.batches.else4")
//      defaultBatches.flatMap { batch =>
//        val filteredRules = batch.rules.filter { rule =>
//          val exclude = excludedRules.contains(rule.ruleName)
//          if (exclude) {
//            println("Unknown.Unknown.then2")
//            logInfo(s"Optimization rule '${
//              rule.ruleName
//            }' is excluded from the optimizer.")
//          } else {
//            println("Unknown.Unknown.else3")
//          }
//          !exclude
//        }
//        if (batch.rules == filteredRules) {
//          println("Unknown.Unknown.then4")
//          Some(batch)
//        } else if (filteredRules.nonEmpty) {
//          println("Unknown.Unknown.then7")
//          println("Unknown.Unknown.elseif5")
//          Some(Batch(batch.name, batch.strategy, filteredRules: _*))
//        } else {
//          println("Unknown.Unknown.else8")
//          println("Unknown.Unknown.else6")
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
//  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(AGGREGATE)) {
//    case agg: Aggregate =>
//      println("EliminateDistinct.apply.case1")
//      agg.transformExpressionsWithPruning(_.containsPattern(AGGREGATE_EXPRESSION)) {
//        case ae: AggregateExpression if ae.isDistinct && isDuplicateAgnostic(ae.aggregateFunction) =>
//          println("Unknown.Unknown.case9")
//          ae.copy(isDistinct = false)
//        case ae: AggregateExpression if ae.isDistinct && agg.child.distinctKeys.exists(_.subsetOf(ExpressionSet(ae.aggregateFunction.children.filterNot(_.foldable)))) =>
//          println("Unknown.Unknown.case10")
//          ae.copy(isDistinct = false)
//      }
//  }
//  def isDuplicateAgnostic(af: AggregateFunction): Boolean = af match {
//    case _: Max =>
//      println("EliminateDistinct.isDuplicateAgnostic.match1")
//      true
//    case _: Min =>
//      println("EliminateDistinct.isDuplicateAgnostic.match2")
//      true
//    case _: BitAndAgg =>
//      println("EliminateDistinct.isDuplicateAgnostic.match3")
//      true
//    case _: BitOrAgg =>
//      println("EliminateDistinct.isDuplicateAgnostic.match4")
//      true
//    case _: CollectSet =>
//      println("EliminateDistinct.isDuplicateAgnostic.match5")
//      true
//    case _: First =>
//      println("EliminateDistinct.isDuplicateAgnostic.match6")
//      true
//    case _: Last =>
//      println("EliminateDistinct.isDuplicateAgnostic.match7")
//      true
//    case _ =>
//      println("EliminateDistinct.isDuplicateAgnostic.match8")
//      false
//  }
//}
//object EliminateAggregateFilter extends Rule[LogicalPlan] {
//  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressionsWithPruning(_.containsAllPatterns(AGGREGATE_EXPRESSION, TRUE_OR_FALSE_LITERAL), ruleId) {
//    case ae @ AggregateExpression(_, _, _, Some(Literal.TrueLiteral), _) =>
//      println("EliminateAggregateFilter.apply.case1")
//      ae.copy(filter = None)
//    case AggregateExpression(af: DeclarativeAggregate, _, _, Some(Literal.FalseLiteral), _) =>
//      println("EliminateAggregateFilter.apply.case2")
//      val initialProject = SafeProjection.create(af.initialValues)
//      val evalProject = SafeProjection.create(af.evaluateExpression :: Nil, af.aggBufferAttributes)
//      val initialBuffer = initialProject(EmptyRow)
//      val internalRow = evalProject(initialBuffer)
//      Literal.create(internalRow.get(0, af.dataType), af.dataType)
//    case AggregateExpression(af: ImperativeAggregate, _, _, Some(Literal.FalseLiteral), _) =>
//      println("EliminateAggregateFilter.apply.case3")
//      val buffer = new SpecificInternalRow(af.aggBufferAttributes.map(_.dataType))
//      af.initialize(buffer)
//      Literal.create(af.eval(buffer), af.dataType)
//  }
//}
//object SimpleTestOptimizer extends SimpleTestOptimizer
//class SimpleTestOptimizer extends Optimizer(new CatalogManager(FakeV2SessionCatalog, new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, EmptyTableFunctionRegistry)))
//object RemoveRedundantAliases extends Rule[LogicalPlan] {
//  private def createAttributeMapping(current: LogicalPlan, next: LogicalPlan): Seq[(Attribute, Attribute)] = {
//    current.output.zip(next.output).filterNot {
//      case (a1, a2) =>
//        println("RemoveRedundantAliases.createAttributeMapping.case1")
//        a1.semanticEquals(a2)
//    }
//  }
//  private def removeRedundantAlias(e: Expression, excludeList: AttributeSet): Expression = e match {
//    case a @ Alias(attr: Attribute, name) if (a.metadata == Metadata.empty || a.metadata == attr.metadata) && name == attr.name && !excludeList.contains(attr) && !excludeList.contains(a) =>
//      println("RemoveRedundantAliases.removeRedundantAlias.match1")
//      attr
//    case a =>
//      println("RemoveRedundantAliases.removeRedundantAlias.match2")
//      a
//  }
//  private def removeRedundantAliases(plan: LogicalPlan, excluded: AttributeSet): LogicalPlan = {
//    if (!plan.containsPattern(ALIAS)) {
//      println("RemoveRedundantAliases.removeRedundantAliases.then1")
//      return plan
//    } else {
//      println("RemoveRedundantAliases.removeRedundantAliases.else2")
//    }
//    plan match {
//      case Subquery(child, correlated) =>
//        println("RemoveRedundantAliases.removeRedundantAliases.match3")
//        Subquery(removeRedundantAliases(child, excluded ++ child.outputSet), correlated)
//      case Join(left, right, joinType, condition, hint) =>
//        println("RemoveRedundantAliases.removeRedundantAliases.match4")
//        val newLeft = removeRedundantAliases(left, excluded ++ right.outputSet)
//        val newRight = removeRedundantAliases(right, excluded ++ newLeft.outputSet)
//        val mapping = AttributeMap(createAttributeMapping(left, newLeft) ++ createAttributeMapping(right, newRight))
//        val newCondition = condition.map(_.transform {
//          case a: Attribute =>
//            println("Unknown.Unknown.case11")
//            mapping.getOrElse(a, a)
//        })
//        Join(newLeft, newRight, joinType, newCondition, hint)
//      case u: Union =>
//        println("RemoveRedundantAliases.removeRedundantAliases.match5")
//        var first = true
//        plan.mapChildren {
//          child => if (first) {
//            println("Unknown.Unknown.then12")
//            first = false
//            removeRedundantAliases(child, excluded ++ child.outputSet)
//          } else {
//            println("Unknown.Unknown.else13")
//            removeRedundantAliases(child, excluded -- u.children.head.outputSet)
//          }
//        }
//      case _ =>
//        println("RemoveRedundantAliases.removeRedundantAliases.match6")
//        val currentNextAttrPairs = mutable.Buffer.empty[(Attribute, Attribute)]
//        val newNode = plan.mapChildren { child =>
//          val newChild = removeRedundantAliases(child, excluded)
//          currentNextAttrPairs ++= createAttributeMapping(child, newChild)
//          newChild
//        }
//        val mapping = AttributeMap(currentNextAttrPairs)
//        val clean: Expression => Expression = plan match {
//          case _: Project =>
//            println("Unknown.Unknown.match14")
//            removeRedundantAlias(_, excluded)
//          case _: Aggregate =>
//            println("Unknown.Unknown.match15")
//            removeRedundantAlias(_, excluded)
//          case _: Window =>
//            println("Unknown.Unknown.match16")
//            removeRedundantAlias(_, excluded)
//          case _ =>
//            println("Unknown.Unknown.match17")
//            identity[Expression]
//        }
//        newNode.mapExpressions {
//          expr => clean(expr.transform {
//            case a: Attribute =>
//              println("Unknown.Unknown.case18")
//              mapping.get(a).map(_.withName(a.name)).getOrElse(a)
//          })
//        }
//    }
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = removeRedundantAliases(plan, AttributeSet.empty)
//}
//object RemoveNoopOperators extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(_.containsAnyPattern(PROJECT, WINDOW), ruleId) {
//    case p @ Project(projectList, child) if child.sameOutput(p) =>
//      println("RemoveNoopOperators.apply.case1")
//      val newChild = child match {
//        case p: Project =>
//          println("Unknown.Unknown.match19")
//          p.copy(projectList = restoreOriginalOutputNames(p.projectList, projectList.map(_.name)))
//        case agg: Aggregate =>
//          println("Unknown.Unknown.match20")
//          agg.copy(aggregateExpressions = restoreOriginalOutputNames(agg.aggregateExpressions, projectList.map(_.name)))
//        case _ =>
//          println("Unknown.Unknown.match21")
//          child
//      }
//      if (newChild.output.zip(projectList).forall {
//        case (a1, a2) =>
//          println("Unknown.Unknown.case24")
//          a1.name == a2.name
//      }) {
//        println("Unknown.Unknown.then22")
//        newChild
//      } else {
//        println("Unknown.Unknown.else23")
//        p
//      }
//    case w: Window if w.windowExpressions.isEmpty =>
//      println("RemoveNoopOperators.apply.case2")
//      w.child
//  }
//}
//object RemoveNoopUnion extends Rule[LogicalPlan] {
//  private def removeAliasOnlyProject(plan: LogicalPlan): LogicalPlan = plan match {
//    case p @ Project(projectList, child) =>
//      println("RemoveNoopUnion.removeAliasOnlyProject.match1")
//      val aliasOnly = projectList.length == child.output.length && projectList.zip(child.output).forall {
//        case (Alias(left: Attribute, _), right) =>
//          println("Unknown.Unknown.case25")
//          left.semanticEquals(right)
//        case (left: Attribute, right) =>
//          println("Unknown.Unknown.case26")
//          left.semanticEquals(right)
//        case _ =>
//          println("Unknown.Unknown.case27")
//          false
//      }
//      if (aliasOnly) {
//        println("Unknown.Unknown.then28")
//        child
//      } else {
//        println("Unknown.Unknown.else29")
//        p
//      }
//    case _ =>
//      println("RemoveNoopUnion.removeAliasOnlyProject.match2")
//      plan
//  }
//  private def simplifyUnion(u: Union): LogicalPlan = {
//    val uniqueChildren = mutable.ArrayBuffer.empty[LogicalPlan]
//    val uniqueChildrenKey = mutable.HashSet.empty[LogicalPlan]
//    u.children.foreach { c =>
//      val key = removeAliasOnlyProject(c).canonicalized
//      if (!uniqueChildrenKey.contains(key)) {
//        println("RemoveNoopUnion.simplifyUnion.then1")
//        uniqueChildren += c
//        uniqueChildrenKey += key
//      } else {
//        println("RemoveNoopUnion.simplifyUnion.else2")
//      }
//    }
//    if (uniqueChildren.size == 1) {
//      println("RemoveNoopUnion.simplifyUnion.then3")
//      u.children.head
//    } else {
//      println("RemoveNoopUnion.simplifyUnion.else4")
//      u.copy(children = uniqueChildren.toSeq)
//    }
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(_.containsAllPatterns(DISTINCT_LIKE, UNION)) {
//    case d @ Distinct(u: Union) =>
//      println("RemoveNoopUnion.apply.case1")
//      d.withNewChildren(Seq(simplifyUnion(u)))
//    case d @ Deduplicate(_, u: Union) =>
//      println("RemoveNoopUnion.apply.case2")
//      d.withNewChildren(Seq(simplifyUnion(u)))
//    case d @ DeduplicateWithinWatermark(_, u: Union) =>
//      println("RemoveNoopUnion.apply.case3")
//      d.withNewChildren(Seq(simplifyUnion(u)))
//  }
//}
//object LimitPushDown extends Rule[LogicalPlan] {
//  private def stripGlobalLimitIfPresent(plan: LogicalPlan): LogicalPlan = {
//    plan match {
//      case GlobalLimit(_, child) =>
//        println("LimitPushDown.stripGlobalLimitIfPresent.match1")
//        child
//      case _ =>
//        println("LimitPushDown.stripGlobalLimitIfPresent.match2")
//        plan
//    }
//  }
//  private def maybePushLocalLimit(limitExp: Expression, plan: LogicalPlan): LogicalPlan = {
//    (limitExp, plan.maxRowsPerPartition) match {
//      case (IntegerLiteral(newLimit), Some(childMaxRows)) if newLimit < childMaxRows =>
//        println("LimitPushDown.maybePushLocalLimit.match1")
//        LocalLimit(limitExp, stripGlobalLimitIfPresent(plan))
//      case (_, None) =>
//        println("LimitPushDown.maybePushLocalLimit.match2")
//        LocalLimit(limitExp, stripGlobalLimitIfPresent(plan))
//      case _ =>
//        println("LimitPushDown.maybePushLocalLimit.match3")
//        plan
//    }
//  }
//  private def pushLocalLimitThroughJoin(limitExpr: Expression, join: Join): Join = {
//    join.joinType match {
//      case RightOuter if join.condition.nonEmpty =>
//        println("LimitPushDown.pushLocalLimitThroughJoin.match1")
//        join.copy(right = maybePushLocalLimit(limitExpr, join.right))
//      case LeftOuter if join.condition.nonEmpty =>
//        println("LimitPushDown.pushLocalLimitThroughJoin.match2")
//        join.copy(left = maybePushLocalLimit(limitExpr, join.left))
//      case _: InnerLike | RightOuter | LeftOuter | FullOuter if join.condition.isEmpty =>
//        println("LimitPushDown.pushLocalLimitThroughJoin.match3")
//        join.copy(left = maybePushLocalLimit(limitExpr, join.left), right = maybePushLocalLimit(limitExpr, join.right))
//      case LeftSemi | LeftAnti if join.condition.isEmpty =>
//        println("LimitPushDown.pushLocalLimitThroughJoin.match4")
//        join.copy(left = maybePushLocalLimit(limitExpr, join.left))
//      case _ =>
//        println("LimitPushDown.pushLocalLimitThroughJoin.match5")
//        join
//    }
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsAnyPattern(LIMIT, LEFT_SEMI_OR_ANTI_JOIN), ruleId) {
//    case LocalLimit(exp, u: Union) =>
//      println("LimitPushDown.apply.case1")
//      LocalLimit(exp, u.copy(children = u.children.map(maybePushLocalLimit(exp, _))))
//    case LocalLimit(exp, join: Join) =>
//      println("LimitPushDown.apply.case2")
//      LocalLimit(exp, pushLocalLimitThroughJoin(exp, join))
//    case LocalLimit(exp, project @ Project(_, join: Join)) =>
//      println("LimitPushDown.apply.case3")
//      LocalLimit(exp, project.copy(child = pushLocalLimitThroughJoin(exp, join)))
//    case Limit(le @ IntegerLiteral(1), a: Aggregate) if a.groupOnly =>
//      println("LimitPushDown.apply.case4")
//      Limit(le, Project(a.aggregateExpressions, LocalLimit(le, a.child)))
//    case Limit(le @ IntegerLiteral(1), p @ Project(_, a: Aggregate)) if a.groupOnly =>
//      println("LimitPushDown.apply.case5")
//      Limit(le, p.copy(child = Project(a.aggregateExpressions, LocalLimit(le, a.child))))
//    case LocalLimit(le, Offset(oe, grandChild)) =>
//      println("LimitPushDown.apply.case6")
//      Offset(oe, LocalLimit(Add(le, oe), grandChild))
//    case j @ Join(_, right, LeftSemiOrAnti(_), None, _) if !right.maxRows.exists(_ <= 1) =>
//      println("LimitPushDown.apply.case7")
//      j.copy(right = maybePushLocalLimit(Literal(1, IntegerType), right))
//    case LocalLimit(le, udf: BatchEvalPython) =>
//      println("LimitPushDown.apply.case8")
//      LocalLimit(le, udf.copy(child = maybePushLocalLimit(le, udf.child)))
//    case LocalLimit(le, p @ Project(_, udf: BatchEvalPython)) =>
//      println("LimitPushDown.apply.case9")
//      LocalLimit(le, p.copy(child = udf.copy(child = maybePushLocalLimit(le, udf.child))))
//    case LocalLimit(le, udf: ArrowEvalPython) =>
//      println("LimitPushDown.apply.case10")
//      LocalLimit(le, udf.copy(child = maybePushLocalLimit(le, udf.child)))
//    case LocalLimit(le, p @ Project(_, udf: ArrowEvalPython)) =>
//      println("LimitPushDown.apply.case11")
//      LocalLimit(le, p.copy(child = udf.copy(child = maybePushLocalLimit(le, udf.child))))
//  }
//}
//object PushProjectionThroughUnion extends Rule[LogicalPlan] {
//  private def buildRewrites(left: LogicalPlan, right: LogicalPlan): AttributeMap[Attribute] = {
//    assert(left.output.size == right.output.size)
//    AttributeMap(left.output.zip(right.output))
//  }
//  private def pushToRight[A <: Expression](e: A, rewrites: AttributeMap[Attribute]) = {
//    val result = e transform {
//      case a: Attribute =>
//        println("Unknown.Unknown.case30")
//        rewrites(a)
//    } match {
//      case Alias(child, alias) =>
//        println("PushProjectionThroughUnion.pushToRight.match1")
//        Alias(child, alias)()
//      case other =>
//        println("PushProjectionThroughUnion.pushToRight.match2")
//        other
//    }
//    result.asInstanceOf[A]
//  }
//  def pushProjectionThroughUnion(projectList: Seq[NamedExpression], u: Union): Seq[LogicalPlan] = {
//    val newFirstChild = Project(projectList, u.children.head)
//    val newOtherChildren = u.children.tail.map { child =>
//      val rewrites = buildRewrites(u.children.head, child)
//      Project(projectList.map(pushToRight(_, rewrites)), child)
//    }
//    newFirstChild +: newOtherChildren
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsAllPatterns(UNION, PROJECT)) {
//    case Project(projectList, u: Union) if projectList.forall(_.deterministic) && u.children.nonEmpty =>
//      println("PushProjectionThroughUnion.apply.case1")
//      u.copy(children = pushProjectionThroughUnion(projectList, u))
//  }
//}
//object ColumnPruning extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = removeProjectBeforeFilter(plan.transformWithPruning(AlwaysProcess.fn, ruleId) {
//    case p @ Project(_, p2: Project) if !p2.outputSet.subsetOf(p.references) =>
//      println("ColumnPruning.apply.case1")
//      p.copy(child = p2.copy(projectList = p2.projectList.filter(p.references.contains)))
//    case p @ Project(_, a: Aggregate) if !a.outputSet.subsetOf(p.references) =>
//      println("ColumnPruning.apply.case2")
//      p.copy(child = a.copy(aggregateExpressions = a.aggregateExpressions.filter(p.references.contains)))
//    case a @ Project(_, e @ Expand(_, _, grandChild)) if !e.outputSet.subsetOf(a.references) =>
//      println("ColumnPruning.apply.case3")
//      val newOutput = e.output.filter(a.references.contains(_))
//      val newProjects = e.projections.map {
//        proj => proj.zip(e.output).filter {
//          case (_, a) =>
//            println("Unknown.Unknown.case31")
//            newOutput.contains(a)
//        }.map(_._1)
//      }
//      a.copy(child = Expand(newProjects, newOutput, grandChild))
//    case p @ Project(_, a @ AttachDistributedSequence(_, grandChild)) if !p.references.contains(a.sequenceAttr) =>
//      println("ColumnPruning.apply.case4")
//      p.copy(child = prunedChild(grandChild, p.references))
//    case d @ DeserializeToObject(_, _, child) if !child.outputSet.subsetOf(d.references) =>
//      println("ColumnPruning.apply.case5")
//      d.copy(child = prunedChild(child, d.references))
//    case a @ Aggregate(_, _, child) if !child.outputSet.subsetOf(a.references) =>
//      println("ColumnPruning.apply.case6")
//      a.copy(child = prunedChild(child, a.references))
//    case f @ FlatMapGroupsInPandas(_, _, _, child) if !child.outputSet.subsetOf(f.references) =>
//      println("ColumnPruning.apply.case7")
//      f.copy(child = prunedChild(child, f.references))
//    case e @ Expand(_, _, child) if !child.outputSet.subsetOf(e.references) =>
//      println("ColumnPruning.apply.case8")
//      e.copy(child = prunedChild(child, e.references))
//    case e @ MergeRows(_, _, _, _, _, _, _, child) if !child.outputSet.subsetOf(e.references) =>
//      println("ColumnPruning.apply.case9")
//      e.copy(child = prunedChild(child, e.references))
//    case p @ Project(_, g: Generate) if p.references != g.outputSet =>
//      println("ColumnPruning.apply.case10")
//      val requiredAttrs = p.references -- g.producedAttributes ++ g.generator.references
//      val newChild = prunedChild(g.child, requiredAttrs)
//      val unrequired = g.generator.references -- p.references
//      val unrequiredIndices = newChild.output.zipWithIndex.filter(t => unrequired.contains(t._1)).map(_._2)
//      p.copy(child = g.copy(child = newChild, unrequiredChildIndex = unrequiredIndices))
//    case GeneratorNestedColumnAliasing(rewrittenPlan) =>
//      println("ColumnPruning.apply.case11")
//      rewrittenPlan
//    case j @ Join(_, right, LeftExistence(_), _, _) =>
//      println("ColumnPruning.apply.case12")
//      j.copy(right = prunedChild(right, j.references))
//    case p @ Project(_, _: SetOperation) =>
//      println("ColumnPruning.apply.case13")
//      p
//    case p @ Project(_, _: Distinct) =>
//      println("ColumnPruning.apply.case14")
//      p
//    case p @ Project(_, u: Union) =>
//      println("ColumnPruning.apply.case15")
//      if (!u.outputSet.subsetOf(p.references)) {
//        println("Unknown.Unknown.then32")
//        val firstChild = u.children.head
//        val newOutput = prunedChild(firstChild, p.references).output
//        val newChildren = u.children.map { p =>
//          val selected = p.output.zipWithIndex.filter {
//            case (a, i) =>
//              println("Unknown.Unknown.case34")
//              newOutput.contains(firstChild.output(i))
//          }.map(_._1)
//          Project(selected, p)
//        }
//        p.copy(child = u.withNewChildren(newChildren))
//      } else {
//        println("Unknown.Unknown.else33")
//        p
//      }
//    case p @ Project(_, w: Window) if !w.windowOutputSet.subsetOf(p.references) =>
//      println("ColumnPruning.apply.case16")
//      val windowExprs = w.windowExpressions.filter(p.references.contains)
//      val newChild = if (windowExprs.isEmpty) {
//        println("Unknown.Unknown.then35")
//        w.child
//      } else {
//        println("Unknown.Unknown.else36")
//        w.copy(windowExpressions = windowExprs)
//      }
//      p.copy(child = newChild)
//    case p @ Project(_, w: WithCTE) =>
//      println("ColumnPruning.apply.case17")
//      if (!w.outputSet.subsetOf(p.references)) {
//        println("Unknown.Unknown.then37")
//        p.copy(child = w.withNewPlan(prunedChild(w.plan, p.references)))
//      } else {
//        println("Unknown.Unknown.else38")
//        p
//      }
//    case p @ Project(_, _: LeafNode) =>
//      println("ColumnPruning.apply.case18")
//      p
//    case NestedColumnAliasing(rewrittenPlan) =>
//      println("ColumnPruning.apply.case19")
//      rewrittenPlan
//    case p @ Project(_, child) if !child.isInstanceOf[Project] =>
//      println("ColumnPruning.apply.case20")
//      val required = child.references ++ p.references
//      if (!child.inputSet.subsetOf(required)) {
//        println("Unknown.Unknown.then39")
//        val newChildren = child.children.map(c => prunedChild(c, required))
//        p.copy(child = child.withNewChildren(newChildren))
//      } else {
//        println("Unknown.Unknown.else40")
//        p
//      }
//  })
//  private def prunedChild(c: LogicalPlan, allReferences: AttributeSet) = if (!c.outputSet.subsetOf(allReferences)) {
//    println("ColumnPruning.prunedChild.then1")
//    Project(c.output.filter(allReferences.contains), c)
//  } else {
//    println("ColumnPruning.prunedChild.else2")
//    c
//  }
//  private def removeProjectBeforeFilter(plan: LogicalPlan): LogicalPlan = plan transformUp {
//    case p1 @ Project(_, f @ Filter(e, p2 @ Project(_, child))) if p2.outputSet.subsetOf(child.outputSet) && p2.projectList.forall(_.isInstanceOf[AttributeReference]) && !hasConflictingAttrsWithSubquery(e, child) =>
//      println("ColumnPruning.removeProjectBeforeFilter.case1")
//      p1.copy(child = f.copy(child = child))
//  }
//  private def hasConflictingAttrsWithSubquery(predicate: Expression, child: LogicalPlan): Boolean = {
//    predicate.find {
//      case s: SubqueryExpression if s.plan.outputSet.intersect(child.outputSet).nonEmpty =>
//        println("ColumnPruning.hasConflictingAttrsWithSubquery.case1")
//        true
//      case _ =>
//        println("ColumnPruning.hasConflictingAttrsWithSubquery.case2")
//        false
//    }.isDefined
//  }
//}
//object CollapseProject extends Rule[LogicalPlan] with AliasHelper {
//  def apply(plan: LogicalPlan): LogicalPlan = {
//    apply(plan, conf.getConf(SQLConf.COLLAPSE_PROJECT_ALWAYS_INLINE))
//  }
//  def apply(plan: LogicalPlan, alwaysInline: Boolean): LogicalPlan = {
//    plan.transformUpWithPruning(_.containsPattern(PROJECT), ruleId) {
//      case p1 @ Project(_, p2: Project) if canCollapseExpressions(p1.projectList, p2.projectList, alwaysInline) =>
//        println("CollapseProject.apply.case1")
//        p2.copy(projectList = buildCleanedProjectList(p1.projectList, p2.projectList))
//      case p @ Project(_, agg: Aggregate) if canCollapseExpressions(p.projectList, agg.aggregateExpressions, alwaysInline) && canCollapseAggregate(p, agg) =>
//        println("CollapseProject.apply.case2")
//        agg.copy(aggregateExpressions = buildCleanedProjectList(p.projectList, agg.aggregateExpressions))
//      case Project(l1, g @ GlobalLimit(_, limit @ LocalLimit(_, p2 @ Project(l2, _)))) if isRenaming(l1, l2) =>
//        println("CollapseProject.apply.case3")
//        val newProjectList = buildCleanedProjectList(l1, l2)
//        g.copy(child = limit.copy(child = p2.copy(projectList = newProjectList)))
//      case Project(l1, limit @ LocalLimit(_, p2 @ Project(l2, _))) if isRenaming(l1, l2) =>
//        println("CollapseProject.apply.case4")
//        val newProjectList = buildCleanedProjectList(l1, l2)
//        limit.copy(child = p2.copy(projectList = newProjectList))
//      case Project(l1, r @ Repartition(_, _, p @ Project(l2, _))) if isRenaming(l1, l2) =>
//        println("CollapseProject.apply.case5")
//        r.copy(child = p.copy(projectList = buildCleanedProjectList(l1, p.projectList)))
//      case Project(l1, s @ Sample(_, _, _, _, p2 @ Project(l2, _))) if isRenaming(l1, l2) =>
//        println("CollapseProject.apply.case6")
//        s.copy(child = p2.copy(projectList = buildCleanedProjectList(l1, p2.projectList)))
//    }
//  }
//  def canCollapseExpressions(consumers: Seq[Expression], producers: Seq[NamedExpression], alwaysInline: Boolean): Boolean = {
//    canCollapseExpressions(consumers, getAliasMap(producers), alwaysInline)
//  }
//  def canCollapseExpressions(consumers: Seq[Expression], producerMap: Map[Attribute, Expression], alwaysInline: Boolean = false): Boolean = {
//    consumers.filter(_.references.exists(producerMap.contains)).flatMap(collectReferences).groupBy(identity).mapValues(_.size).forall {
//      case (reference, count) =>
//        println("CollapseProject.canCollapseExpressions.case1")
//        val producer = producerMap.getOrElse(reference, reference)
//        val relatedConsumers = consumers.filter(_.references.contains(reference))
//        def cheapToInlineProducer: Boolean = trimAliases(producer) match {
//          case e @ (_: CreateNamedStruct | _: UpdateFields | _: CreateMap | _: CreateArray) =>
//            println("Unknown.cheapToInlineProducer.match1")
//            var nonCheapAccessSeen = false
//            def nonCheapAccessVisitor(): Boolean = {
//              try {
//                nonCheapAccessSeen
//              } finally {
//                nonCheapAccessSeen = true
//              }
//            }
//            !relatedConsumers.exists(findNonCheapAccesses(_, reference, e, nonCheapAccessVisitor))
//          case other =>
//            println("Unknown.cheapToInlineProducer.match2")
//            isCheap(other)
//        }
//        producer.deterministic && (count == 1 || alwaysInline || cheapToInlineProducer)
//    }
//  }
//  private object ExtractOnlyRef {
//    @scala.annotation.tailrec def unapply(expr: Expression): Option[Attribute] = expr match {
//      case a: Alias =>
//        println("ExtractOnlyRef.unapply.match1")
//        unapply(a.child)
//      case e: ExtractValue =>
//        println("ExtractOnlyRef.unapply.match2")
//        unapply(e.children.head)
//      case a: Attribute =>
//        println("ExtractOnlyRef.unapply.match3")
//        Some(a)
//      case _ =>
//        println("ExtractOnlyRef.unapply.match4")
//        None
//    }
//  }
//  private def inlineReference(expr: Expression, ref: Attribute, refExpr: Expression): Expression = {
//    expr.transformUp {
//      case a: Attribute if a.semanticEquals(ref) =>
//        println("CollapseProject.inlineReference.case1")
//        refExpr
//    }
//  }
//  private object SimplifyExtractValueExecutor extends RuleExecutor[LogicalPlan] { override val batches = Batch("SimplifyExtractValueOps", FixedPoint(10), SimplifyExtractValueOps, ConstantFolding, SimplifyConditionals) :: Nil }
//  private def simplifyExtractValues(expr: Expression): Expression = {
//    val fakePlan = Project(Seq(Alias(expr, "fake")()), LocalRelation(Nil))
//    SimplifyExtractValueExecutor.execute(fakePlan).asInstanceOf[Project].projectList.head.asInstanceOf[Alias].child
//  }
//  private def findNonCheapAccesses(consumer: Expression, ref: Attribute, refExpr: Expression, nonCheapAccessVisitor: () => Boolean): Boolean = consumer match {
//    case attr: Attribute if attr.semanticEquals(ref) =>
//      println("CollapseProject.findNonCheapAccesses.match1")
//      nonCheapAccessVisitor()
//    case e @ ExtractOnlyRef(attr) if attr.semanticEquals(ref) =>
//      println("CollapseProject.findNonCheapAccesses.match2")
//      val finalExpr = simplifyExtractValues(inlineReference(e, ref, refExpr))
//      !isCheap(finalExpr) && nonCheapAccessVisitor()
//    case _ =>
//      println("CollapseProject.findNonCheapAccesses.match3")
//      consumer.children.exists(findNonCheapAccesses(_, ref, refExpr, nonCheapAccessVisitor))
//  }
//  private def canCollapseAggregate(p: Project, a: Aggregate): Boolean = {
//    p.projectList.forall(_.collect {
//      case s: ScalarSubquery if s.outerAttrs.nonEmpty =>
//        println("CollapseProject.canCollapseAggregate.case1")
//        s
//    }.isEmpty)
//  }
//  def buildCleanedProjectList(upper: Seq[NamedExpression], lower: Seq[NamedExpression]): Seq[NamedExpression] = {
//    val aliases = getAliasMap(lower)
//    upper.map(replaceAliasButKeepName(_, aliases))
//  }
//  def isCheap(e: Expression): Boolean = e match {
//    case _: Attribute | _: OuterReference =>
//      println("CollapseProject.isCheap.match1")
//      true
//    case _ if e.foldable =>
//      println("CollapseProject.isCheap.match2")
//      true
//    case _: PythonUDF =>
//      println("CollapseProject.isCheap.match3")
//      true
//    case _: Alias | _: ExtractValue =>
//      println("CollapseProject.isCheap.match4")
//      e.children.forall(isCheap)
//    case _ =>
//      println("CollapseProject.isCheap.match5")
//      false
//  }
//  private def collectReferences(e: Expression): Seq[Attribute] = e.collect {
//    case a: Attribute =>
//      println("CollapseProject.collectReferences.case1")
//      a
//  }
//  private def isRenaming(list1: Seq[NamedExpression], list2: Seq[NamedExpression]): Boolean = {
//    list1.length == list2.length && list1.zip(list2).forall {
//      case (e1, e2) if e1.semanticEquals(e2) =>
//        println("CollapseProject.isRenaming.case1")
//        true
//      case (Alias(a: Attribute, _), b) if a.metadata == Metadata.empty && a.name == b.name =>
//        println("CollapseProject.isRenaming.case2")
//        true
//      case _ =>
//        println("CollapseProject.isRenaming.case3")
//        false
//    }
//  }
//}
//object CollapseRepartition extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(_.containsAnyPattern(REPARTITION_OPERATION, REBALANCE_PARTITIONS), ruleId) {
//    case r @ Repartition(_, _, child: RepartitionOperation) =>
//      println("CollapseRepartition.apply.case1")
//      (r.shuffle, child.shuffle) match {
//        case (false, true) =>
//          println("Unknown.Unknown.match41")
//          if (r.numPartitions >= child.numPartitions) {
//            println("Unknown.Unknown.then43")
//            child
//          } else {
//            println("Unknown.Unknown.else44")
//            r
//          }
//        case _ =>
//          println("Unknown.Unknown.match42")
//          r.copy(child = child.child)
//      }
//    case r @ RepartitionByExpression(_, child @ (Sort(_, true, _) | _: RepartitionOperation), _, _) =>
//      println("CollapseRepartition.apply.case2")
//      r.withNewChildren(child.children)
//    case r @ RebalancePartitions(_, child @ (_: Sort | _: RepartitionOperation), _, _) =>
//      println("CollapseRepartition.apply.case3")
//      r.withNewChildren(child.children)
//    case r @ RebalancePartitions(_, child: RebalancePartitions, _, _) =>
//      println("CollapseRepartition.apply.case4")
//      r.withNewChildren(child.children)
//  }
//}
//object OptimizeRepartition extends Rule[LogicalPlan] {
//  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(REPARTITION_OPERATION), ruleId) {
//    case r @ RepartitionByExpression(partitionExpressions, _, numPartitions, _) if partitionExpressions.nonEmpty && partitionExpressions.forall(_.foldable) && numPartitions.isEmpty =>
//      println("OptimizeRepartition.apply.case1")
//      r.copy(optNumPartitions = Some(1))
//  }
//}
//object OptimizeWindowFunctions extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveExpressionsWithPruning(_.containsPattern(WINDOW_EXPRESSION), ruleId) {
//    case we @ WindowExpression(AggregateExpression(first: First, _, _, _, _), WindowSpecDefinition(_, orderSpec, frameSpecification: SpecifiedWindowFrame)) if orderSpec.nonEmpty && frameSpecification.frameType == RowFrame && frameSpecification.lower == UnboundedPreceding && (frameSpecification.upper == UnboundedFollowing || frameSpecification.upper == CurrentRow) =>
//      println("OptimizeWindowFunctions.apply.case1")
//      we.copy(windowFunction = NthValue(first.child, Literal(1), first.ignoreNulls))
//  }
//}
//object CollapseWindow extends Rule[LogicalPlan] {
//  private def specCompatible(s1: Seq[Expression], s2: Seq[Expression]): Boolean = {
//    s1.length == s2.length && s1.zip(s2).forall(e => e._1.semanticEquals(e._2))
//  }
//  private def windowsCompatible(w1: Window, w2: Window): Boolean = {
//    specCompatible(w1.partitionSpec, w2.partitionSpec) && specCompatible(w1.orderSpec, w2.orderSpec) && w1.references.intersect(w2.windowOutputSet).isEmpty && w1.windowExpressions.nonEmpty && w2.windowExpressions.nonEmpty && WindowFunctionType.functionType(w1.windowExpressions.head) == WindowFunctionType.functionType(w2.windowExpressions.head)
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(_.containsPattern(WINDOW), ruleId) {
//    case w1 @ Window(we1, _, _, w2 @ Window(we2, _, _, grandChild)) if windowsCompatible(w1, w2) =>
//      println("CollapseWindow.apply.case1")
//      w1.copy(windowExpressions = we2 ++ we1, child = grandChild)
//    case w1 @ Window(we1, _, _, Project(pl, w2 @ Window(we2, _, _, grandChild))) if windowsCompatible(w1, w2) && w1.references.subsetOf(grandChild.outputSet) =>
//      println("CollapseWindow.apply.case2")
//      Project(pl ++ w1.windowOutputSet, w1.copy(windowExpressions = we2 ++ we1, child = grandChild))
//  }
//}
//object TransposeWindow extends Rule[LogicalPlan] {
//  private def compatiblePartitions(ps1: Seq[Expression], ps2: Seq[Expression]): Boolean = {
//    ps1.length < ps2.length && ps1.forall {
//      expr1 => ps2.exists(expr1.semanticEquals)
//    }
//  }
//  private def windowsCompatible(w1: Window, w2: Window): Boolean = {
//    w1.references.intersect(w2.windowOutputSet).isEmpty && w1.expressions.forall(_.deterministic) && w2.expressions.forall(_.deterministic) && compatiblePartitions(w1.partitionSpec, w2.partitionSpec)
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(_.containsPattern(WINDOW), ruleId) {
//    case w1 @ Window(_, _, _, w2 @ Window(_, _, _, grandChild)) if windowsCompatible(w1, w2) =>
//      println("TransposeWindow.apply.case1")
//      Project(w1.output, w2.copy(child = w1.copy(child = grandChild)))
//    case w1 @ Window(_, _, _, Project(pl, w2 @ Window(_, _, _, grandChild))) if windowsCompatible(w1, w2) && w1.references.subsetOf(grandChild.outputSet) =>
//      println("TransposeWindow.apply.case2")
//      Project(pl ++ w1.windowOutputSet, w2.copy(child = w1.copy(child = grandChild)))
//  }
//}
//object InferFiltersFromGenerate extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(_.containsPattern(GENERATE)) {
//    case generate @ Generate(g, _, false, _, _, _) if canInferFilters(g) =>
//      println("InferFiltersFromGenerate.apply.case1")
//      assert(g.children.length == 1)
//      val input = g.children.head
//      if (input.isInstanceOf[Attribute]) {
//        println("Unknown.Unknown.then45")
//        val inferredFilters = ExpressionSet(Seq(GreaterThan(Size(input), Literal(0)), IsNotNull(input))) -- generate.child.constraints
//        if (inferredFilters.nonEmpty) {
//          println("Unknown.Unknown.then47")
//          generate.copy(child = Filter(inferredFilters.reduce(And), generate.child))
//        } else {
//          println("Unknown.Unknown.else48")
//          generate
//        }
//      } else {
//        println("Unknown.Unknown.else46")
//        generate
//      }
//  }
//  private def canInferFilters(g: Generator): Boolean = g match {
//    case _: ExplodeBase =>
//      println("InferFiltersFromGenerate.canInferFilters.match1")
//      true
//    case _: Inline =>
//      println("InferFiltersFromGenerate.canInferFilters.match2")
//      true
//    case _ =>
//      println("InferFiltersFromGenerate.canInferFilters.match3")
//      false
//  }
//}
//object InferFiltersFromConstraints extends Rule[LogicalPlan] with PredicateHelper with ConstraintHelper {
//  def apply(plan: LogicalPlan): LogicalPlan = {
//    if (conf.constraintPropagationEnabled) {
//      println("InferFiltersFromConstraints.apply.then1")
//      inferFilters(plan)
//    } else {
//      println("InferFiltersFromConstraints.apply.else2")
//      plan
//    }
//  }
//  private def inferFilters(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsAnyPattern(FILTER, JOIN)) {
//    case filter @ Filter(condition, child) =>
//      println("InferFiltersFromConstraints.inferFilters.case1")
//      val newFilters = filter.constraints -- (child.constraints ++ splitConjunctivePredicates(condition))
//      if (newFilters.nonEmpty) {
//        println("Unknown.Unknown.then49")
//        Filter(And(newFilters.reduce(And), condition), child)
//      } else {
//        println("Unknown.Unknown.else50")
//        filter
//      }
//    case join @ Join(left, right, joinType, conditionOpt, _) =>
//      println("InferFiltersFromConstraints.inferFilters.case2")
//      joinType match {
//        case _: InnerLike | LeftSemi =>
//          println("Unknown.Unknown.match51")
//          val allConstraints = getAllConstraints(left, right, conditionOpt)
//          val newLeft = inferNewFilter(left, allConstraints)
//          val newRight = inferNewFilter(right, allConstraints)
//          join.copy(left = newLeft, right = newRight)
//        case RightOuter =>
//          println("Unknown.Unknown.match52")
//          val allConstraints = getAllConstraints(left, right, conditionOpt)
//          val newLeft = inferNewFilter(left, allConstraints)
//          join.copy(left = newLeft)
//        case LeftOuter | LeftAnti =>
//          println("Unknown.Unknown.match53")
//          val allConstraints = getAllConstraints(left, right, conditionOpt)
//          val newRight = inferNewFilter(right, allConstraints)
//          join.copy(right = newRight)
//        case _ =>
//          println("Unknown.Unknown.match54")
//          join
//      }
//  }
//  private def getAllConstraints(left: LogicalPlan, right: LogicalPlan, conditionOpt: Option[Expression]): ExpressionSet = {
//    val baseConstraints = left.constraints.union(right.constraints).union(ExpressionSet(conditionOpt.map(splitConjunctivePredicates).getOrElse(Nil)))
//    baseConstraints.union(inferAdditionalConstraints(baseConstraints))
//  }
//  private def inferNewFilter(plan: LogicalPlan, constraints: ExpressionSet): LogicalPlan = {
//    val newPredicates = constraints.union(constructIsNotNullConstraints(constraints, plan.output)).filter {
//      c => c.references.nonEmpty && c.references.subsetOf(plan.outputSet) && c.deterministic
//    } -- plan.constraints
//    if (newPredicates.isEmpty) {
//      println("InferFiltersFromConstraints.inferNewFilter.then1")
//      plan
//    } else {
//      println("InferFiltersFromConstraints.inferNewFilter.else2")
//      Filter(newPredicates.reduce(And), plan)
//    }
//  }
//}
//object CombineUnions extends Rule[LogicalPlan] {
//  import CollapseProject.{ buildCleanedProjectList, canCollapseExpressions }
//  import PushProjectionThroughUnion.pushProjectionThroughUnion
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformDownWithPruning(_.containsAnyPattern(UNION, DISTINCT_LIKE), ruleId) {
//    case u: Union =>
//      println("CombineUnions.apply.case1")
//      flattenUnion(u, false)
//    case Distinct(u: Union) =>
//      println("CombineUnions.apply.case2")
//      Distinct(flattenUnion(u, true))
//    case Deduplicate(keys: Seq[Attribute], u: Union) if AttributeSet(keys) == u.outputSet =>
//      println("CombineUnions.apply.case3")
//      Deduplicate(keys, flattenUnion(u, true))
//    case DeduplicateWithinWatermark(keys: Seq[Attribute], u: Union) if AttributeSet(keys) == u.outputSet =>
//      println("CombineUnions.apply.case4")
//      DeduplicateWithinWatermark(keys, flattenUnion(u, true))
//  }
//  private def flattenUnion(union: Union, flattenDistinct: Boolean): Union = {
//    val topByName = union.byName
//    val topAllowMissingCol = union.allowMissingCol
//    val stack = mutable.Stack[LogicalPlan](union)
//    val flattened = mutable.ArrayBuffer.empty[LogicalPlan]
//    while (stack.nonEmpty) {
//      stack.pop() match {
//        case p1 @ Project(_, p2: Project) if canCollapseExpressions(p1.projectList, p2.projectList, alwaysInline = false) && !p1.projectList.exists(SubqueryExpression.hasCorrelatedSubquery) && !p2.projectList.exists(SubqueryExpression.hasCorrelatedSubquery) =>
//          println("CombineUnions.flattenUnion.match1")
//          val newProjectList = buildCleanedProjectList(p1.projectList, p2.projectList)
//          stack.pushAll(Seq(p2.copy(projectList = newProjectList)))
//        case Distinct(Union(children, byName, allowMissingCol)) if flattenDistinct && byName == topByName && allowMissingCol == topAllowMissingCol =>
//          println("CombineUnions.flattenUnion.match2")
//          stack.pushAll(children.reverse)
//        case Deduplicate(keys: Seq[Attribute], u: Union) if flattenDistinct && u.byName == topByName && u.allowMissingCol == topAllowMissingCol && AttributeSet(keys) == u.outputSet =>
//          println("CombineUnions.flattenUnion.match3")
//          stack.pushAll(u.children.reverse)
//        case Union(children, byName, allowMissingCol) if byName == topByName && allowMissingCol == topAllowMissingCol =>
//          println("CombineUnions.flattenUnion.match4")
//          stack.pushAll(children.reverse)
//        case Project(projectList, Distinct(u @ Union(children, byName, allowMissingCol))) if projectList.forall(_.deterministic) && children.nonEmpty && flattenDistinct && byName == topByName && allowMissingCol == topAllowMissingCol =>
//          println("CombineUnions.flattenUnion.match5")
//          stack.pushAll(pushProjectionThroughUnion(projectList, u).reverse)
//        case Project(projectList, Deduplicate(keys: Seq[Attribute], u: Union)) if projectList.forall(_.deterministic) && flattenDistinct && u.byName == topByName && u.allowMissingCol == topAllowMissingCol && AttributeSet(keys) == u.outputSet =>
//          println("CombineUnions.flattenUnion.match6")
//          stack.pushAll(pushProjectionThroughUnion(projectList, u).reverse)
//        case Project(projectList, u @ Union(children, byName, allowMissingCol)) if projectList.forall(_.deterministic) && children.nonEmpty && byName == topByName && allowMissingCol == topAllowMissingCol =>
//          println("CombineUnions.flattenUnion.match7")
//          stack.pushAll(pushProjectionThroughUnion(projectList, u).reverse)
//        case child =>
//          println("CombineUnions.flattenUnion.match8")
//          flattened += child
//      }
//    }
//    union.copy(children = flattened.toSeq)
//  }
//}
//object CombineFilters extends Rule[LogicalPlan] with PredicateHelper {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(FILTER), ruleId)(applyLocally)
//  val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
//    case Filter(fc, nf @ Filter(nc, grandChild)) if nc.deterministic =>
//      println("CombineFilters.Unknown.case1")
//      val (combineCandidates, nonDeterministic) = splitConjunctivePredicates(fc).partition(_.deterministic)
//      val mergedFilter = (ExpressionSet(combineCandidates) -- ExpressionSet(splitConjunctivePredicates(nc))).reduceOption(And) match {
//        case Some(ac) =>
//          println("Unknown.Unknown.match55")
//          Filter(And(nc, ac), grandChild)
//        case None =>
//          println("Unknown.Unknown.match56")
//          nf
//      }
//      nonDeterministic.reduceOption(And).map(c => Filter(c, mergedFilter)).getOrElse(mergedFilter)
//  }
//}
//object EliminateSorts extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(SORT))(applyLocally)
//  private val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
//    case s @ Sort(orders, _, child) if orders.isEmpty || orders.exists(_.child.foldable) =>
//      println("EliminateSorts.Unknown.case1")
//      val newOrders = orders.filterNot(_.child.foldable)
//      if (newOrders.isEmpty) {
//        println("Unknown.Unknown.then57")
//        applyLocally.lift(child).getOrElse(child)
//      } else {
//        println("Unknown.Unknown.else58")
//        s.copy(order = newOrders)
//      }
//    case Sort(orders, false, child) if SortOrder.orderingSatisfies(child.outputOrdering, orders) =>
//      println("EliminateSorts.Unknown.case2")
//      applyLocally.lift(child).getOrElse(child)
//    case s @ Sort(_, global, child) =>
//      println("EliminateSorts.Unknown.case3")
//      s.copy(child = recursiveRemoveSort(child, global))
//    case j @ Join(originLeft, originRight, _, cond, _) if cond.forall(_.deterministic) =>
//      println("EliminateSorts.Unknown.case4")
//      j.copy(left = recursiveRemoveSort(originLeft, true), right = recursiveRemoveSort(originRight, true))
//    case g @ Aggregate(_, aggs, originChild) if isOrderIrrelevantAggs(aggs) =>
//      println("EliminateSorts.Unknown.case5")
//      g.copy(child = recursiveRemoveSort(originChild, true))
//  }
//  private def recursiveRemoveSort(plan: LogicalPlan, canRemoveGlobalSort: Boolean): LogicalPlan = {
//    if (!plan.containsPattern(SORT)) {
//      println("EliminateSorts.recursiveRemoveSort.then1")
//      return plan
//    } else {
//      println("EliminateSorts.recursiveRemoveSort.else2")
//    }
//    plan match {
//      case Sort(_, global, child) if canRemoveGlobalSort || !global =>
//        println("EliminateSorts.recursiveRemoveSort.match3")
//        recursiveRemoveSort(child, canRemoveGlobalSort)
//      case Sort(sortOrder, true, child) =>
//        println("EliminateSorts.recursiveRemoveSort.match4")
//        RepartitionByExpression(sortOrder, recursiveRemoveSort(child, true), None)
//      case other if canEliminateSort(other) =>
//        println("EliminateSorts.recursiveRemoveSort.match5")
//        other.withNewChildren(other.children.map(c => recursiveRemoveSort(c, canRemoveGlobalSort)))
//      case other if canEliminateGlobalSort(other) =>
//        println("EliminateSorts.recursiveRemoveSort.match6")
//        other.withNewChildren(other.children.map(c => recursiveRemoveSort(c, true)))
//      case _ =>
//        println("EliminateSorts.recursiveRemoveSort.match7")
//        plan
//    }
//  }
//  private def canEliminateSort(plan: LogicalPlan): Boolean = plan match {
//    case p: Project =>
//      println("EliminateSorts.canEliminateSort.match1")
//      p.projectList.forall(_.deterministic)
//    case f: Filter =>
//      println("EliminateSorts.canEliminateSort.match2")
//      f.condition.deterministic
//    case _: LocalLimit =>
//      println("EliminateSorts.canEliminateSort.match3")
//      true
//    case _ =>
//      println("EliminateSorts.canEliminateSort.match4")
//      false
//  }
//  private def canEliminateGlobalSort(plan: LogicalPlan): Boolean = plan match {
//    case r: RepartitionByExpression =>
//      println("EliminateSorts.canEliminateGlobalSort.match1")
//      r.partitionExpressions.forall(_.deterministic)
//    case r: RebalancePartitions =>
//      println("EliminateSorts.canEliminateGlobalSort.match2")
//      r.partitionExpressions.forall(_.deterministic)
//    case _: Repartition =>
//      println("EliminateSorts.canEliminateGlobalSort.match3")
//      true
//    case _ =>
//      println("EliminateSorts.canEliminateGlobalSort.match4")
//      false
//  }
//  private def isOrderIrrelevantAggs(aggs: Seq[NamedExpression]): Boolean = {
//    def isOrderIrrelevantAggFunction(func: AggregateFunction): Boolean = func match {
//      case _: Min | _: Max | _: Count | _: BitAggregate =>
//        println("EliminateSorts.isOrderIrrelevantAggFunction.match1")
//        true
//      case _: Sum | _: Average | _: CentralMomentAgg =>
//        println("EliminateSorts.isOrderIrrelevantAggFunction.match2")
//        !Seq(FloatType, DoubleType).exists(e => DataTypeUtils.sameType(e, func.children.head.dataType))
//      case _ =>
//        println("EliminateSorts.isOrderIrrelevantAggFunction.match3")
//        false
//    }
//    def checkValidAggregateExpression(expr: Expression): Boolean = expr match {
//      case _: AttributeReference =>
//        println("EliminateSorts.checkValidAggregateExpression.match1")
//        true
//      case ae: AggregateExpression =>
//        println("EliminateSorts.checkValidAggregateExpression.match2")
//        isOrderIrrelevantAggFunction(ae.aggregateFunction)
//      case _: UserDefinedExpression =>
//        println("EliminateSorts.checkValidAggregateExpression.match3")
//        false
//      case e =>
//        println("EliminateSorts.checkValidAggregateExpression.match4")
//        e.children.forall(checkValidAggregateExpression)
//    }
//    aggs.forall(checkValidAggregateExpression)
//  }
//}
//object PruneFilters extends Rule[LogicalPlan] with PredicateHelper {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(FILTER), ruleId) {
//    case Filter(Literal(true, BooleanType), child) =>
//      println("PruneFilters.apply.case1")
//      child
//    case Filter(Literal(null, _), child) =>
//      println("PruneFilters.apply.case2")
//      LocalRelation(child.output, data = Seq.empty, isStreaming = plan.isStreaming)
//    case Filter(Literal(false, BooleanType), child) =>
//      println("PruneFilters.apply.case3")
//      LocalRelation(child.output, data = Seq.empty, isStreaming = plan.isStreaming)
//    case f @ Filter(fc, p: LogicalPlan) =>
//      println("PruneFilters.apply.case4")
//      val (prunedPredicates, remainingPredicates) = splitConjunctivePredicates(fc).partition {
//        cond => cond.deterministic && p.constraints.contains(cond)
//      }
//      if (prunedPredicates.isEmpty) {
//        println("Unknown.Unknown.then59")
//        f
//      } else if (remainingPredicates.isEmpty) {
//        println("Unknown.Unknown.then62")
//        println("Unknown.Unknown.elseif60")
//        p
//      } else {
//        println("Unknown.Unknown.else63")
//        println("Unknown.Unknown.else61")
//        val newCond = remainingPredicates.reduce(And)
//        Filter(newCond, p)
//      }
//  }
//}
//object PushDownPredicates extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsAnyPattern(FILTER, JOIN)) {
//    CombineFilters.applyLocally.orElse(PushPredicateThroughNonJoin.applyLocally).orElse(PushPredicateThroughJoin.applyLocally)
//  }
//}
//object PushPredicateThroughNonJoin extends Rule[LogicalPlan] with PredicateHelper {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform applyLocally
//  val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
//    case Filter(condition, project @ Project(fields, grandChild)) if fields.forall(_.deterministic) && canPushThroughCondition(grandChild, condition) =>
//      println("PushPredicateThroughNonJoin.Unknown.case1")
//      val aliasMap = getAliasMap(project)
//      project.copy(child = Filter(replaceAlias(condition, aliasMap), grandChild))
//    case filter @ Filter(condition, aggregate: Aggregate) if aggregate.aggregateExpressions.forall(_.deterministic) && aggregate.groupingExpressions.nonEmpty =>
//      println("PushPredicateThroughNonJoin.Unknown.case2")
//      val aliasMap = getAliasMap(aggregate)
//      val (candidates, nonDeterministic) = splitConjunctivePredicates(condition).partition(_.deterministic)
//      val (pushDown, rest) = candidates.partition { cond =>
//        val replaced = replaceAlias(cond, aliasMap)
//        cond.references.nonEmpty && replaced.references.subsetOf(aggregate.child.outputSet)
//      }
//      val stayUp = rest ++ nonDeterministic
//      if (pushDown.nonEmpty) {
//        println("Unknown.Unknown.then64")
//        val pushDownPredicate = pushDown.reduce(And)
//        val replaced = replaceAlias(pushDownPredicate, aliasMap)
//        val newAggregate = aggregate.copy(child = Filter(replaced, aggregate.child))
//        if (stayUp.isEmpty) {
//          println("Unknown.Unknown.then66")
//          newAggregate
//        } else {
//          println("Unknown.Unknown.else67")
//          Filter(stayUp.reduce(And), newAggregate)
//        }
//      } else {
//        println("Unknown.Unknown.else65")
//        filter
//      }
//    case filter @ Filter(condition, w: Window) if w.partitionSpec.forall(_.isInstanceOf[AttributeReference]) =>
//      println("PushPredicateThroughNonJoin.Unknown.case3")
//      val partitionAttrs = AttributeSet(w.partitionSpec.flatMap(_.references))
//      val (candidates, nonDeterministic) = splitConjunctivePredicates(condition).partition(_.deterministic)
//      val (pushDown, rest) = candidates.partition {
//        cond => cond.references.subsetOf(partitionAttrs)
//      }
//      val stayUp = rest ++ nonDeterministic
//      if (pushDown.nonEmpty) {
//        println("Unknown.Unknown.then68")
//        val pushDownPredicate = pushDown.reduce(And)
//        val newWindow = w.copy(child = Filter(pushDownPredicate, w.child))
//        if (stayUp.isEmpty) {
//          println("Unknown.Unknown.then70")
//          newWindow
//        } else {
//          println("Unknown.Unknown.else71")
//          Filter(stayUp.reduce(And), newWindow)
//        }
//      } else {
//        println("Unknown.Unknown.else69")
//        filter
//      }
//    case filter @ Filter(condition, union: Union) =>
//      println("PushPredicateThroughNonJoin.Unknown.case4")
//      val (pushDown, stayUp) = splitConjunctivePredicates(condition).partition(_.deterministic)
//      if (pushDown.nonEmpty) {
//        println("Unknown.Unknown.then72")
//        val pushDownCond = pushDown.reduceLeft(And)
//        val output = union.output
//        val newGrandChildren = union.children.map { grandchild =>
//          val newCond = pushDownCond transform {
//            case e if output.exists(_.semanticEquals(e)) =>
//              println("Unknown.Unknown.case74")
//              grandchild.output(output.indexWhere(_.semanticEquals(e)))
//          }
//          assert(newCond.references.subsetOf(grandchild.outputSet))
//          Filter(newCond, grandchild)
//        }
//        val newUnion = union.withNewChildren(newGrandChildren)
//        if (stayUp.nonEmpty) {
//          println("Unknown.Unknown.then75")
//          Filter(stayUp.reduceLeft(And), newUnion)
//        } else {
//          println("Unknown.Unknown.else76")
//          newUnion
//        }
//      } else {
//        println("Unknown.Unknown.else73")
//        filter
//      }
//    case filter @ Filter(condition, watermark: EventTimeWatermark) =>
//      println("PushPredicateThroughNonJoin.Unknown.case5")
//      val (pushDown, stayUp) = splitConjunctivePredicates(condition).partition {
//        p => p.deterministic && !p.references.contains(watermark.eventTime)
//      }
//      if (pushDown.nonEmpty) {
//        println("Unknown.Unknown.then77")
//        val pushDownPredicate = pushDown.reduceLeft(And)
//        val newWatermark = watermark.copy(child = Filter(pushDownPredicate, watermark.child))
//        if (stayUp.isEmpty) {
//          println("Unknown.Unknown.then79")
//          newWatermark
//        } else {
//          println("Unknown.Unknown.else80")
//          Filter(stayUp.reduceLeft(And), newWatermark)
//        }
//      } else {
//        println("Unknown.Unknown.else78")
//        filter
//      }
//    case filter @ Filter(_, u: UnaryNode) if canPushThrough(u) && u.expressions.forall(_.deterministic) =>
//      println("PushPredicateThroughNonJoin.Unknown.case6")
//      pushDownPredicate(filter, u.child) {
//        predicate => u.withNewChildren(Seq(Filter(predicate, u.child)))
//      }
//  }
//  def canPushThrough(p: UnaryNode): Boolean = p match {
//    case _: AppendColumns =>
//      println("PushPredicateThroughNonJoin.canPushThrough.match1")
//      true
//    case _: Distinct =>
//      println("PushPredicateThroughNonJoin.canPushThrough.match2")
//      true
//    case _: Generate =>
//      println("PushPredicateThroughNonJoin.canPushThrough.match3")
//      true
//    case _: Pivot =>
//      println("PushPredicateThroughNonJoin.canPushThrough.match4")
//      true
//    case _: RepartitionByExpression =>
//      println("PushPredicateThroughNonJoin.canPushThrough.match5")
//      true
//    case _: Repartition =>
//      println("PushPredicateThroughNonJoin.canPushThrough.match6")
//      true
//    case _: RebalancePartitions =>
//      println("PushPredicateThroughNonJoin.canPushThrough.match7")
//      true
//    case _: ScriptTransformation =>
//      println("PushPredicateThroughNonJoin.canPushThrough.match8")
//      true
//    case _: Sort =>
//      println("PushPredicateThroughNonJoin.canPushThrough.match9")
//      true
//    case _: BatchEvalPython =>
//      println("PushPredicateThroughNonJoin.canPushThrough.match10")
//      true
//    case _: ArrowEvalPython =>
//      println("PushPredicateThroughNonJoin.canPushThrough.match11")
//      true
//    case _: Expand =>
//      println("PushPredicateThroughNonJoin.canPushThrough.match12")
//      true
//    case _ =>
//      println("PushPredicateThroughNonJoin.canPushThrough.match13")
//      false
//  }
//  private def pushDownPredicate(filter: Filter, grandchild: LogicalPlan)(insertFilter: Expression => LogicalPlan): LogicalPlan = {
//    val (candidates, nonDeterministic) = splitConjunctivePredicates(filter.condition).partition(_.deterministic)
//    val (pushDown, rest) = candidates.partition {
//      cond => cond.references.subsetOf(grandchild.outputSet)
//    }
//    val stayUp = rest ++ nonDeterministic
//    if (pushDown.nonEmpty) {
//      println("PushPredicateThroughNonJoin.pushDownPredicate.then1")
//      val newChild = insertFilter(pushDown.reduceLeft(And))
//      if (stayUp.nonEmpty) {
//        println("Unknown.Unknown.then81")
//        Filter(stayUp.reduceLeft(And), newChild)
//      } else {
//        println("Unknown.Unknown.else82")
//        newChild
//      }
//    } else {
//      println("PushPredicateThroughNonJoin.pushDownPredicate.else2")
//      filter
//    }
//  }
//  private def canPushThroughCondition(plan: LogicalPlan, condition: Expression): Boolean = {
//    val attributes = plan.outputSet
//    !condition.exists {
//      case s: SubqueryExpression =>
//        println("PushPredicateThroughNonJoin.canPushThroughCondition.case1")
//        s.plan.outputSet.intersect(attributes).nonEmpty
//      case _ =>
//        println("PushPredicateThroughNonJoin.canPushThroughCondition.case2")
//        false
//    }
//  }
//}
//object PushPredicateThroughJoin extends Rule[LogicalPlan] with PredicateHelper {
//  private def split(condition: Seq[Expression], left: LogicalPlan, right: LogicalPlan) = {
//    val (pushDownCandidates, nonDeterministic) = condition.partition(_.deterministic)
//    val (leftEvaluateCondition, rest) = pushDownCandidates.partition(_.references.subsetOf(left.outputSet))
//    val (rightEvaluateCondition, commonCondition) = rest.partition(expr => expr.references.subsetOf(right.outputSet))
//    (leftEvaluateCondition, rightEvaluateCondition, commonCondition ++ nonDeterministic)
//  }
//  private def canPushThrough(joinType: JoinType): Boolean = joinType match {
//    case _: InnerLike | LeftSemi | RightOuter | LeftOuter | LeftAnti | ExistenceJoin(_) =>
//      println("PushPredicateThroughJoin.canPushThrough.match1")
//      true
//    case _ =>
//      println("PushPredicateThroughJoin.canPushThrough.match2")
//      false
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform applyLocally
//  val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
//    case f @ Filter(filterCondition, Join(left, right, joinType, joinCondition, hint)) if canPushThrough(joinType) =>
//      println("PushPredicateThroughJoin.Unknown.case1")
//      val (leftFilterConditions, rightFilterConditions, commonFilterCondition) = split(splitConjunctivePredicates(filterCondition), left, right)
//      joinType match {
//        case _: InnerLike =>
//          println("Unknown.Unknown.match83")
//          val newLeft = leftFilterConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
//          val newRight = rightFilterConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
//          val (newJoinConditions, others) = commonFilterCondition.partition(canEvaluateWithinJoin)
//          val newJoinCond = (newJoinConditions ++ joinCondition).reduceLeftOption(And)
//          val join = Join(newLeft, newRight, joinType, newJoinCond, hint)
//          if (others.nonEmpty) {
//            println("Unknown.Unknown.then87")
//            Filter(others.reduceLeft(And), join)
//          } else {
//            println("Unknown.Unknown.else88")
//            join
//          }
//        case RightOuter =>
//          println("Unknown.Unknown.match84")
//          val newLeft = left
//          val newRight = rightFilterConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
//          val newJoinCond = joinCondition
//          val newJoin = Join(newLeft, newRight, RightOuter, newJoinCond, hint)
//          (leftFilterConditions ++ commonFilterCondition).reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
//        case LeftOuter | LeftExistence(_) =>
//          println("Unknown.Unknown.match85")
//          val newLeft = leftFilterConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
//          val newRight = right
//          val newJoinCond = joinCondition
//          val newJoin = Join(newLeft, newRight, joinType, newJoinCond, hint)
//          (rightFilterConditions ++ commonFilterCondition).reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
//        case other =>
//          println("Unknown.Unknown.match86")
//          throw new IllegalStateException(s"Unexpected join type: $other")
//      }
//    case j @ Join(left, right, joinType, joinCondition, hint) if canPushThrough(joinType) =>
//      println("PushPredicateThroughJoin.Unknown.case2")
//      val (leftJoinConditions, rightJoinConditions, commonJoinCondition) = split(joinCondition.map(splitConjunctivePredicates).getOrElse(Nil), left, right)
//      joinType match {
//        case _: InnerLike | LeftSemi =>
//          println("Unknown.Unknown.match89")
//          val newLeft = leftJoinConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
//          val newRight = rightJoinConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
//          val newJoinCond = commonJoinCondition.reduceLeftOption(And)
//          Join(newLeft, newRight, joinType, newJoinCond, hint)
//        case RightOuter =>
//          println("Unknown.Unknown.match90")
//          val newLeft = leftJoinConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
//          val newRight = right
//          val newJoinCond = (rightJoinConditions ++ commonJoinCondition).reduceLeftOption(And)
//          Join(newLeft, newRight, RightOuter, newJoinCond, hint)
//        case LeftOuter | LeftAnti | ExistenceJoin(_) =>
//          println("Unknown.Unknown.match91")
//          val newLeft = left
//          val newRight = rightJoinConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
//          val newJoinCond = (leftJoinConditions ++ commonJoinCondition).reduceLeftOption(And)
//          Join(newLeft, newRight, joinType, newJoinCond, hint)
//        case other =>
//          println("Unknown.Unknown.match92")
//          throw new IllegalStateException(s"Unexpected join type: $other")
//      }
//  }
//}
//object EliminateLimits extends Rule[LogicalPlan] {
//  private def canEliminate(limitExpr: Expression, child: LogicalPlan): Boolean = {
//    limitExpr.foldable && child.maxRows.exists {
//      _ <= (limitExpr.eval().asInstanceOf[Int])
//    }
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformDownWithPruning(_.containsPattern(LIMIT), ruleId) {
//    case Limit(l, child) if canEliminate(l, child) =>
//      println("EliminateLimits.apply.case1")
//      child
//    case GlobalLimit(l, child) if canEliminate(l, child) =>
//      println("EliminateLimits.apply.case2")
//      child
//    case LocalLimit(IntegerLiteral(0), child) =>
//      println("EliminateLimits.apply.case3")
//      LocalRelation(child.output, data = Seq.empty, isStreaming = child.isStreaming)
//    case GlobalLimit(IntegerLiteral(0), child) =>
//      println("EliminateLimits.apply.case4")
//      LocalRelation(child.output, data = Seq.empty, isStreaming = child.isStreaming)
//    case GlobalLimit(le, GlobalLimit(ne, grandChild)) =>
//      println("EliminateLimits.apply.case5")
//      GlobalLimit(Literal(Least(Seq(ne, le)).eval().asInstanceOf[Int]), grandChild)
//    case LocalLimit(le, LocalLimit(ne, grandChild)) =>
//      println("EliminateLimits.apply.case6")
//      LocalLimit(Literal(Least(Seq(ne, le)).eval().asInstanceOf[Int]), grandChild)
//    case Limit(le, Limit(ne, grandChild)) =>
//      println("EliminateLimits.apply.case7")
//      Limit(Literal(Least(Seq(ne, le)).eval().asInstanceOf[Int]), grandChild)
//  }
//}
//object EliminateOffsets extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case Offset(oe, child) if oe.foldable && oe.eval().asInstanceOf[Int] == 0 =>
//      println("EliminateOffsets.apply.case1")
//      child
//    case Offset(oe, child) if oe.foldable && child.maxRows.exists(_ <= (oe.eval().asInstanceOf[Int])) =>
//      println("EliminateOffsets.apply.case2")
//      LocalRelation(child.output, data = Seq.empty, isStreaming = child.isStreaming)
//    case Offset(oe1, Offset(oe2, child)) =>
//      println("EliminateOffsets.apply.case3")
//      Offset(Add(oe1, oe2), child)
//  }
//}
//object CheckCartesianProducts extends Rule[LogicalPlan] with PredicateHelper {
//  def isCartesianProduct(join: Join): Boolean = {
//    val conditions = join.condition.map(splitConjunctivePredicates).getOrElse(Nil)
//    conditions match {
//      case Seq(Literal.FalseLiteral) | Seq(Literal(null, BooleanType)) =>
//        println("CheckCartesianProducts.isCartesianProduct.match1")
//        false
//      case _ =>
//        println("CheckCartesianProducts.isCartesianProduct.match2")
//        !conditions.map(_.references).exists(refs => refs.exists(join.left.outputSet.contains) && refs.exists(join.right.outputSet.contains))
//    }
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = if (conf.crossJoinEnabled) {
//    println("CheckCartesianProducts.apply.then1")
//    plan
//  } else {
//    println("CheckCartesianProducts.apply.else2")
//    plan.transformWithPruning(_.containsAnyPattern(INNER_LIKE_JOIN, OUTER_JOIN)) {
//      case j @ Join(left, right, Inner | LeftOuter | RightOuter | FullOuter, _, _) if isCartesianProduct(j) =>
//        println("Unknown.Unknown.case93")
//        throw QueryCompilationErrors.joinConditionMissingOrTrivialError(j, left, right)
//    }
//  }
//}
//object DecimalAggregates extends Rule[LogicalPlan] {
//  import Decimal.MAX_LONG_DIGITS
//  private val MAX_DOUBLE_DIGITS = 15
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsAnyPattern(SUM, AVERAGE), ruleId) {
//    case q: LogicalPlan =>
//      println("DecimalAggregates.apply.case1")
//      q.transformExpressionsDownWithPruning(_.containsAnyPattern(SUM, AVERAGE), ruleId) {
//        case we @ WindowExpression(ae @ AggregateExpression(af, _, _, _, _), _) =>
//          println("Unknown.Unknown.case94")
//          af match {
//            case Sum(e @ DecimalExpression(prec, scale), _) if prec + 10 <= MAX_LONG_DIGITS =>
//              println("Unknown.Unknown.match96")
//              MakeDecimal(we.copy(windowFunction = ae.copy(aggregateFunction = Sum(UnscaledValue(e)))), prec + 10, scale)
//            case Average(e @ DecimalExpression(prec, scale), _) if prec + 4 <= MAX_DOUBLE_DIGITS =>
//              println("Unknown.Unknown.match97")
//              val newAggExpr = we.copy(windowFunction = ae.copy(aggregateFunction = Average(UnscaledValue(e))))
//              Cast(Divide(newAggExpr, Literal.create(math.pow(10.0d, scale), DoubleType)), DecimalType(prec + 4, scale + 4), Option(conf.sessionLocalTimeZone))
//            case _ =>
//              println("Unknown.Unknown.match98")
//              we
//          }
//        case ae @ AggregateExpression(af, _, _, _, _) =>
//          println("Unknown.Unknown.case95")
//          af match {
//            case Sum(e @ DecimalExpression(prec, scale), _) if prec + 10 <= MAX_LONG_DIGITS =>
//              println("Unknown.Unknown.match99")
//              MakeDecimal(ae.copy(aggregateFunction = Sum(UnscaledValue(e))), prec + 10, scale)
//            case Average(e @ DecimalExpression(prec, scale), _) if prec + 4 <= MAX_DOUBLE_DIGITS =>
//              println("Unknown.Unknown.match100")
//              val newAggExpr = ae.copy(aggregateFunction = Average(UnscaledValue(e)))
//              Cast(Divide(newAggExpr, Literal.create(math.pow(10.0d, scale), DoubleType)), DecimalType(prec + 4, scale + 4), Option(conf.sessionLocalTimeZone))
//            case _ =>
//              println("Unknown.Unknown.match101")
//              ae
//          }
//      }
//  }
//}
//object ConvertToLocalRelation extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(LOCAL_RELATION), ruleId) {
//    case Project(projectList, LocalRelation(output, data, isStreaming)) if !projectList.exists(hasUnevaluableExpr) =>
//      println("ConvertToLocalRelation.apply.case1")
//      val projection = new InterpretedMutableProjection(projectList, output)
//      projection.initialize(0)
//      LocalRelation(projectList.map(_.toAttribute), data.map(projection(_).copy()), isStreaming)
//    case Limit(IntegerLiteral(limit), LocalRelation(output, data, isStreaming)) =>
//      println("ConvertToLocalRelation.apply.case2")
//      LocalRelation(output, data.take(limit), isStreaming)
//    case Filter(condition, LocalRelation(output, data, isStreaming)) if !hasUnevaluableExpr(condition) =>
//      println("ConvertToLocalRelation.apply.case3")
//      val predicate = Predicate.create(condition, output)
//      predicate.initialize(0)
//      LocalRelation(output, data.filter(row => predicate.eval(row)), isStreaming)
//  }
//  private def hasUnevaluableExpr(expr: Expression): Boolean = {
//    expr.exists(e => e.isInstanceOf[Unevaluable] && !e.isInstanceOf[AttributeReference])
//  }
//}
//object ReplaceDistinctWithAggregate extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(DISTINCT_LIKE), ruleId) {
//    case Distinct(child) =>
//      println("ReplaceDistinctWithAggregate.apply.case1")
//      Aggregate(child.output, child.output, child)
//  }
//}
//object ReplaceDeduplicateWithAggregate extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transformUpWithNewOutput {
//    case d @ Deduplicate(keys, child) if !child.isStreaming =>
//      println("ReplaceDeduplicateWithAggregate.apply.case1")
//      val keyExprIds = keys.map(_.exprId)
//      val aggCols = child.output.map {
//        attr => if (keyExprIds.contains(attr.exprId)) {
//          println("Unknown.Unknown.then102")
//          attr
//        } else {
//          println("Unknown.Unknown.else103")
//          Alias(new First(attr).toAggregateExpression(), attr.name)()
//        }
//      }
//      val nonemptyKeys = if (keys.isEmpty) {
//        println("Unknown.Unknown.then104")
//        Literal(1) :: Nil
//      } else {
//        println("Unknown.Unknown.else105")
//        keys
//      }
//      val newAgg = Aggregate(nonemptyKeys, aggCols, child)
//      val attrMapping = d.output.zip(newAgg.output)
//      newAgg -> attrMapping
//  }
//}
//object ReplaceIntersectWithSemiJoin extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(INTERSECT), ruleId) {
//    case Intersect(left, right, false) =>
//      println("ReplaceIntersectWithSemiJoin.apply.case1")
//      assert(left.output.size == right.output.size)
//      val joinCond = left.output.zip(right.output).map {
//        case (l, r) =>
//          println("Unknown.Unknown.case106")
//          EqualNullSafe(l, r)
//      }
//      Distinct(Join(left, right, LeftSemi, joinCond.reduceLeftOption(And), JoinHint.NONE))
//  }
//}
//object ReplaceExceptWithAntiJoin extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(EXCEPT), ruleId) {
//    case Except(left, right, false) =>
//      println("ReplaceExceptWithAntiJoin.apply.case1")
//      assert(left.output.size == right.output.size)
//      val joinCond = left.output.zip(right.output).map {
//        case (l, r) =>
//          println("Unknown.Unknown.case107")
//          EqualNullSafe(l, r)
//      }
//      Distinct(Join(left, right, LeftAnti, joinCond.reduceLeftOption(And), JoinHint.NONE))
//  }
//}
//object RewriteExceptAll extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(EXCEPT), ruleId) {
//    case Except(left, right, true) =>
//      println("RewriteExceptAll.apply.case1")
//      assert(left.output.size == right.output.size)
//      val newColumnLeft = Alias(Literal(1L), "vcol")()
//      val newColumnRight = Alias(Literal(-1L), "vcol")()
//      val modifiedLeftPlan = Project(Seq(newColumnLeft) ++ left.output, left)
//      val modifiedRightPlan = Project(Seq(newColumnRight) ++ right.output, right)
//      val unionPlan = Union(modifiedLeftPlan, modifiedRightPlan)
//      val aggSumCol = Alias(Sum(unionPlan.output.head.toAttribute).toAggregateExpression(), "sum")()
//      val aggOutputColumns = left.output ++ Seq(aggSumCol)
//      val aggregatePlan = Aggregate(left.output, aggOutputColumns, unionPlan)
//      val filteredAggPlan = Filter(GreaterThan(aggSumCol.toAttribute, Literal(0L)), aggregatePlan)
//      val genRowPlan = Generate(ReplicateRows(Seq(aggSumCol.toAttribute) ++ left.output), unrequiredChildIndex = Nil, outer = false, qualifier = None, left.output, filteredAggPlan)
//      Project(left.output, genRowPlan)
//  }
//}
//object RewriteIntersectAll extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(INTERSECT), ruleId) {
//    case Intersect(left, right, true) =>
//      println("RewriteIntersectAll.apply.case1")
//      assert(left.output.size == right.output.size)
//      val trueVcol1 = Alias(Literal(true), "vcol1")()
//      val nullVcol1 = Alias(Literal(null, BooleanType), "vcol1")()
//      val trueVcol2 = Alias(Literal(true), "vcol2")()
//      val nullVcol2 = Alias(Literal(null, BooleanType), "vcol2")()
//      val leftPlanWithAddedVirtualCols = Project(Seq(trueVcol1, nullVcol2) ++ left.output, left)
//      val rightPlanWithAddedVirtualCols = Project(Seq(nullVcol1, trueVcol2) ++ right.output, right)
//      val unionPlan = Union(leftPlanWithAddedVirtualCols, rightPlanWithAddedVirtualCols)
//      val vCol1AggrExpr = Alias(Count(unionPlan.output(0)).toAggregateExpression(), "vcol1_count")()
//      val vCol2AggrExpr = Alias(Count(unionPlan.output(1)).toAggregateExpression(), "vcol2_count")()
//      val ifExpression = Alias(If(GreaterThan(vCol1AggrExpr.toAttribute, vCol2AggrExpr.toAttribute), vCol2AggrExpr.toAttribute, vCol1AggrExpr.toAttribute), "min_count")()
//      val aggregatePlan = Aggregate(left.output, Seq(vCol1AggrExpr, vCol2AggrExpr) ++ left.output, unionPlan)
//      val filterPlan = Filter(And(GreaterThanOrEqual(vCol1AggrExpr.toAttribute, Literal(1L)), GreaterThanOrEqual(vCol2AggrExpr.toAttribute, Literal(1L))), aggregatePlan)
//      val projectMinPlan = Project(left.output ++ Seq(ifExpression), filterPlan)
//      val genRowPlan = Generate(ReplicateRows(Seq(ifExpression.toAttribute) ++ left.output), unrequiredChildIndex = Nil, outer = false, qualifier = None, left.output, projectMinPlan)
//      Project(left.output, genRowPlan)
//  }
//}
//object RemoveLiteralFromGroupExpressions extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(AGGREGATE), ruleId) {
//    case a @ Aggregate(grouping, _, _) if grouping.nonEmpty =>
//      println("RemoveLiteralFromGroupExpressions.apply.case1")
//      val newGrouping = grouping.filter(!_.foldable)
//      if (newGrouping.nonEmpty) {
//        println("Unknown.Unknown.then108")
//        a.copy(groupingExpressions = newGrouping)
//      } else {
//        println("Unknown.Unknown.else109")
//        a.copy(groupingExpressions = Seq(Literal(0, IntegerType)))
//      }
//  }
//}
//object GenerateOptimization extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformDownWithPruning(_.containsAllPatterns(PROJECT, GENERATE), ruleId) {
//    case p @ Project(_, g: Generate) if p.references.isEmpty && (g.generator.isInstanceOf[ExplodeBase]) =>
//      println("GenerateOptimization.apply.case1")
//      g.generator.children.head.dataType match {
//        case ArrayType(StructType(fields), containsNull) if fields.length > 1 =>
//          println("Unknown.Unknown.match110")
//          val sortedFields = fields.zipWithIndex.sortBy(f => f._1.dataType.defaultSize)
//          val extractor = GetArrayStructFields(g.generator.children.head, sortedFields(0)._1, sortedFields(0)._2, fields.length, containsNull || sortedFields(0)._1.nullable)
//          val rewrittenG = g.transformExpressions {
//            case e: ExplodeBase =>
//              println("Unknown.Unknown.case112")
//              e.withNewChildren(Seq(extractor))
//          }
//          val updatedGeneratorOutput = rewrittenG.generatorOutput.zip(toAttributes(rewrittenG.generator.elementSchema)).map {
//            case (oldAttr, newAttr) =>
//              println("Unknown.Unknown.case113")
//              newAttr.withExprId(oldAttr.exprId).withName(oldAttr.name)
//          }
//          assert(updatedGeneratorOutput.length == rewrittenG.generatorOutput.length, "Updated generator output must have the same length " + "with original generator output.")
//          val updatedGenerate = rewrittenG.copy(generatorOutput = updatedGeneratorOutput)
//          p.withNewChildren(Seq(updatedGenerate))
//        case _ =>
//          println("Unknown.Unknown.match111")
//          p
//      }
//  }
//}
//object RemoveRepetitionFromGroupExpressions extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(AGGREGATE), ruleId) {
//    case a @ Aggregate(grouping, _, _) if grouping.size > 1 =>
//      println("RemoveRepetitionFromGroupExpressions.apply.case1")
//      val newGrouping = ExpressionSet(grouping).toSeq
//      if (newGrouping.size == grouping.size) {
//        println("Unknown.Unknown.then114")
//        a
//      } else {
//        println("Unknown.Unknown.else115")
//        a.copy(groupingExpressions = newGrouping)
//      }
//  }
//}