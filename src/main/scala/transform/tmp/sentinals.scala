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
//        case (sp, rule) if sentinel(true, "FinishAnalysis.apply.case1") =>
//          Rule.coverage += "FinishAnalysis.apply.case1"
//          rule.apply(sp)
//      }.transformAllExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
//        case s: SubqueryExpression if sentinel(true, "FinishAnalysis.apply.case2") =>
//          Rule.coverage += "FinishAnalysis.apply.case2"
//          val Subquery(newPlan, _) = apply(Subquery.fromExpression(s))
//          s.withNewPlan(newPlan)
//      }
//    }
//  }
//  object OptimizeSubqueries extends Rule[LogicalPlan] {
//    private def removeTopLevelSort(plan: LogicalPlan): LogicalPlan = {
//      if (sentinel(true, "OptimizeSubqueries.removeTopLevelSort.then1") && !plan.containsPattern(SORT)) {
//        Rule.coverage += "OptimizeSubqueries.removeTopLevelSort.then1"
//        return plan
//      } else {
//        Rule.coverage += "OptimizeSubqueries.removeTopLevelSort.else2"
//        sentinel(true, "OptimizeSubqueries.removeTopLevelSort.else2")
//      }
//      plan match {
//        case Sort(_, _, child) if sentinel(true, "OptimizeSubqueries.removeTopLevelSort.match3") =>
//          Rule.coverage += "OptimizeSubqueries.removeTopLevelSort.match3"
//          child
//        case Project(fields, child) if sentinel(true, "OptimizeSubqueries.removeTopLevelSort.match4") =>
//          Rule.coverage += "OptimizeSubqueries.removeTopLevelSort.match4"
//          Project(fields, removeTopLevelSort(child))
//        case other if sentinel(true, "OptimizeSubqueries.removeTopLevelSort.match5") =>
//          Rule.coverage += "OptimizeSubqueries.removeTopLevelSort.match5"
//          other
//      }
//    }
//    def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION), ruleId) {
//      case d: DynamicPruningSubquery if sentinel(true, "OptimizeSubqueries.apply.case1") =>
//        Rule.coverage += "OptimizeSubqueries.apply.case1"
//        d
//      case s: SubqueryExpression if sentinel(true, "OptimizeSubqueries.apply.case2") =>
//        Rule.coverage += "OptimizeSubqueries.apply.case2"
//        val Subquery(newPlan, _) = Optimizer.this.execute(Subquery.fromExpression(s))
//        s.withNewPlan(removeTopLevelSort(newPlan))
//    }
//  }
//  object UpdateCTERelationStats extends Rule[LogicalPlan] {
//    override def apply(plan: LogicalPlan): LogicalPlan = {
//      if (sentinel(true, "UpdateCTERelationStats.apply.then1") && (!plan.isInstanceOf[Subquery] && plan.containsPattern(CTE))) {
//        Rule.coverage += "UpdateCTERelationStats.apply.then1"
//        val statsMap = mutable.HashMap.empty[Long, Statistics]
//        updateCTEStats(plan, statsMap)
//      } else {
//        Rule.coverage += "UpdateCTERelationStats.apply.else2"
//        sentinel(true, "UpdateCTERelationStats.apply.else2")
//        plan
//      }
//    }
//    private def updateCTEStats(plan: LogicalPlan, statsMap: mutable.HashMap[Long, Statistics]): LogicalPlan = plan match {
//      case WithCTE(child, cteDefs) if sentinel(true, "UpdateCTERelationStats.updateCTEStats.match1") =>
//        Rule.coverage += "UpdateCTERelationStats.updateCTEStats.match1"
//        val newDefs = cteDefs.map { cteDef =>
//          val newDef = updateCTEStats(cteDef, statsMap)
//          statsMap.put(cteDef.id, newDef.stats)
//          newDef.asInstanceOf[CTERelationDef]
//        }
//        WithCTE(updateCTEStats(child, statsMap), newDefs)
//      case c: CTERelationRef if sentinel(true, "UpdateCTERelationStats.updateCTEStats.match2") =>
//        Rule.coverage += "UpdateCTERelationStats.updateCTEStats.match2"
//        statsMap.get(c.cteId).map(s => c.withNewStats(Some(s))).getOrElse(c)
//      case _ if sentinel(true, "UpdateCTERelationStats.updateCTEStats.match3") && plan.containsPattern(CTE) =>
//        Rule.coverage += "UpdateCTERelationStats.updateCTEStats.match3"
//        plan.withNewChildren(plan.children.map(child => updateCTEStats(child, statsMap))).transformExpressionsWithPruning(_.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
//          case e: SubqueryExpression if sentinel(true, "UpdateCTERelationStats.updateCTEStats.case1") =>
//            Rule.coverage += "UpdateCTERelationStats.updateCTEStats.case1"
//            e.withNewPlan(updateCTEStats(e.plan, statsMap))
//        }
//      case _ if sentinel(true, "UpdateCTERelationStats.updateCTEStats.match4") =>
//        Rule.coverage += "UpdateCTERelationStats.updateCTEStats.match4"
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
//      if (sentinel(true, "Optimizer.batches.then1") && nonExcludable) {
//        Rule.coverage += "Optimizer.batches.then1"
//        logWarning(s"Optimization rule '${
//          ruleName
//        }' was not excluded from the optimizer " + (s"because this rule is a non-excludable rule."))
//      } else {
//        Rule.coverage += "Optimizer.batches.else2"
//        sentinel(true, "Optimizer.batches.else2")
//      }
//      !nonExcludable
//    }
//    if (sentinel(true, "Optimizer.batches.then3") && excludedRules.isEmpty) {
//      Rule.coverage += "Optimizer.batches.then3"
//      defaultBatches
//    } else {
//      Rule.coverage += "Optimizer.batches.else4"
//      sentinel(true, "Optimizer.batches.else4")
//      defaultBatches.flatMap { batch =>
//        val filteredRules = batch.rules.filter { rule =>
//          val exclude = excludedRules.contains(rule.ruleName)
//          if (sentinel(true, "Optimizer.batches.then2") && exclude) {
//            Rule.coverage += "Optimizer.batches.then2"
//            logInfo(s"Optimization rule '${
//              rule.ruleName
//            }' is excluded from the optimizer.")
//          } else {
//            Rule.coverage += "Optimizer.batches.else3"
//            sentinel(true, "Optimizer.batches.else3")
//          }
//          !exclude
//        }
//        if (sentinel(true, "Optimizer.batches.then4") && batch.rules == filteredRules) {
//          Rule.coverage += "Optimizer.batches.then4"
//          Some(batch)
//        } else if (sentinel(true, "Optimizer.batches.elseif5") && (sentinel(true, "Optimizer.batches.elseif5") && filteredRules.nonEmpty)) {
//          Rule.coverage += "Optimizer.batches.elseif5"
//          Some(Batch(batch.name, batch.strategy, filteredRules: _*))
//        } else {
//          Rule.coverage += "Optimizer.batches.else6"
//          sentinel(true, "Optimizer.batches.else6")
//          sentinel(true, "Optimizer.batches.else6")
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
//    case agg: Aggregate if sentinel(true, "EliminateDistinct.apply.case1") =>
//      Rule.coverage += "EliminateDistinct.apply.case1"
//      agg.transformExpressionsWithPruning(_.containsPattern(AGGREGATE_EXPRESSION)) {
//        case ae: AggregateExpression if sentinel(true, "EliminateDistinct.apply.case9") && (ae.isDistinct && isDuplicateAgnostic(ae.aggregateFunction)) =>
//          Rule.coverage += "EliminateDistinct.apply.case9"
//          ae.copy(isDistinct = false)
//        case ae: AggregateExpression if sentinel(true, "EliminateDistinct.apply.case10") && (ae.isDistinct && agg.child.distinctKeys.exists(_.subsetOf(ExpressionSet(ae.aggregateFunction.children.filterNot(_.foldable))))) =>
//          Rule.coverage += "EliminateDistinct.apply.case10"
//          ae.copy(isDistinct = false)
//      }
//  }
//  def isDuplicateAgnostic(af: AggregateFunction): Boolean = af match {
//    case _: Max if sentinel(true, "EliminateDistinct.isDuplicateAgnostic.match1") =>
//      Rule.coverage += "EliminateDistinct.isDuplicateAgnostic.match1"
//      true
//    case _: Min if sentinel(true, "EliminateDistinct.isDuplicateAgnostic.match2") =>
//      Rule.coverage += "EliminateDistinct.isDuplicateAgnostic.match2"
//      true
//    case _: BitAndAgg if sentinel(true, "EliminateDistinct.isDuplicateAgnostic.match3") =>
//      Rule.coverage += "EliminateDistinct.isDuplicateAgnostic.match3"
//      true
//    case _: BitOrAgg if sentinel(true, "EliminateDistinct.isDuplicateAgnostic.match4") =>
//      Rule.coverage += "EliminateDistinct.isDuplicateAgnostic.match4"
//      true
//    case _: CollectSet if sentinel(true, "EliminateDistinct.isDuplicateAgnostic.match5") =>
//      Rule.coverage += "EliminateDistinct.isDuplicateAgnostic.match5"
//      true
//    case _: First if sentinel(true, "EliminateDistinct.isDuplicateAgnostic.match6") =>
//      Rule.coverage += "EliminateDistinct.isDuplicateAgnostic.match6"
//      true
//    case _: Last if sentinel(true, "EliminateDistinct.isDuplicateAgnostic.match7") =>
//      Rule.coverage += "EliminateDistinct.isDuplicateAgnostic.match7"
//      true
//    case _ if sentinel(true, "EliminateDistinct.isDuplicateAgnostic.match8") =>
//      Rule.coverage += "EliminateDistinct.isDuplicateAgnostic.match8"
//      false
//  }
//}
//object EliminateAggregateFilter extends Rule[LogicalPlan] {
//  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressionsWithPruning(_.containsAllPatterns(AGGREGATE_EXPRESSION, TRUE_OR_FALSE_LITERAL), ruleId) {
//    case ae @ AggregateExpression(_, _, _, Some(Literal.TrueLiteral), _) if sentinel(true, "EliminateAggregateFilter.apply.case1") =>
//      Rule.coverage += "EliminateAggregateFilter.apply.case1"
//      ae.copy(filter = None)
//    case AggregateExpression(af: DeclarativeAggregate, _, _, Some(Literal.FalseLiteral), _) if sentinel(true, "EliminateAggregateFilter.apply.case2") =>
//      Rule.coverage += "EliminateAggregateFilter.apply.case2"
//      val initialProject = SafeProjection.create(af.initialValues)
//      val evalProject = SafeProjection.create(af.evaluateExpression :: Nil, af.aggBufferAttributes)
//      val initialBuffer = initialProject(EmptyRow)
//      val internalRow = evalProject(initialBuffer)
//      Literal.create(internalRow.get(0, af.dataType), af.dataType)
//    case AggregateExpression(af: ImperativeAggregate, _, _, Some(Literal.FalseLiteral), _) if sentinel(true, "EliminateAggregateFilter.apply.case3") =>
//      Rule.coverage += "EliminateAggregateFilter.apply.case3"
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
//      case (a1, a2) if sentinel(true, "RemoveRedundantAliases.createAttributeMapping.case1") =>
//        Rule.coverage += "RemoveRedundantAliases.createAttributeMapping.case1"
//        a1.semanticEquals(a2)
//    }
//  }
//  private def removeRedundantAlias(e: Expression, excludeList: AttributeSet): Expression = e match {
//    case a @ Alias(attr: Attribute, name) if sentinel(true, "RemoveRedundantAliases.removeRedundantAlias.match1") && ((a.metadata == Metadata.empty || a.metadata == attr.metadata) && name == attr.name && !excludeList.contains(attr) && !excludeList.contains(a)) =>
//      Rule.coverage += "RemoveRedundantAliases.removeRedundantAlias.match1"
//      attr
//    case a if sentinel(true, "RemoveRedundantAliases.removeRedundantAlias.match2") =>
//      Rule.coverage += "RemoveRedundantAliases.removeRedundantAlias.match2"
//      a
//  }
//  private def removeRedundantAliases(plan: LogicalPlan, excluded: AttributeSet): LogicalPlan = {
//    if (sentinel(true, "RemoveRedundantAliases.removeRedundantAliases.then1") && !plan.containsPattern(ALIAS)) {
//      Rule.coverage += "RemoveRedundantAliases.removeRedundantAliases.then1"
//      return plan
//    } else {
//      Rule.coverage += "RemoveRedundantAliases.removeRedundantAliases.else2"
//      sentinel(true, "RemoveRedundantAliases.removeRedundantAliases.else2")
//    }
//    plan match {
//      case Subquery(child, correlated) if sentinel(true, "RemoveRedundantAliases.removeRedundantAliases.match3") =>
//        Rule.coverage += "RemoveRedundantAliases.removeRedundantAliases.match3"
//        Subquery(removeRedundantAliases(child, excluded ++ child.outputSet), correlated)
//      case Join(left, right, joinType, condition, hint) if sentinel(true, "RemoveRedundantAliases.removeRedundantAliases.match4") =>
//        Rule.coverage += "RemoveRedundantAliases.removeRedundantAliases.match4"
//        val newLeft = removeRedundantAliases(left, excluded ++ right.outputSet)
//        val newRight = removeRedundantAliases(right, excluded ++ newLeft.outputSet)
//        val mapping = AttributeMap(createAttributeMapping(left, newLeft) ++ createAttributeMapping(right, newRight))
//        val newCondition = condition.map(_.transform {
//          case a: Attribute if sentinel(true, "RemoveRedundantAliases.removeRedundantAliases.case11") =>
//            Rule.coverage += "RemoveRedundantAliases.removeRedundantAliases.case11"
//            mapping.getOrElse(a, a)
//        })
//        Join(newLeft, newRight, joinType, newCondition, hint)
//      case u: Union if sentinel(true, "RemoveRedundantAliases.removeRedundantAliases.match5") =>
//        Rule.coverage += "RemoveRedundantAliases.removeRedundantAliases.match5"
//        var first = true
//        plan.mapChildren {
//          child => if (sentinel(true, "RemoveRedundantAliases.removeRedundantAliases.then12") && first) {
//            Rule.coverage += "RemoveRedundantAliases.removeRedundantAliases.then12"
//            first = false
//            removeRedundantAliases(child, excluded ++ child.outputSet)
//          } else {
//            Rule.coverage += "RemoveRedundantAliases.removeRedundantAliases.else13"
//            sentinel(true, "RemoveRedundantAliases.removeRedundantAliases.else13")
//            removeRedundantAliases(child, excluded -- u.children.head.outputSet)
//          }
//        }
//      case _ if sentinel(true, "RemoveRedundantAliases.removeRedundantAliases.match6") =>
//        Rule.coverage += "RemoveRedundantAliases.removeRedundantAliases.match6"
//        val currentNextAttrPairs = mutable.Buffer.empty[(Attribute, Attribute)]
//        val newNode = plan.mapChildren { child =>
//          val newChild = removeRedundantAliases(child, excluded)
//          currentNextAttrPairs ++= createAttributeMapping(child, newChild)
//          newChild
//        }
//        val mapping = AttributeMap(currentNextAttrPairs)
//        val clean: Expression => Expression = plan match {
//          case _: Project if sentinel(true, "RemoveRedundantAliases.removeRedundantAliases.match14") =>
//            Rule.coverage += "RemoveRedundantAliases.removeRedundantAliases.match14"
//            removeRedundantAlias(_, excluded)
//          case _: Aggregate if sentinel(true, "RemoveRedundantAliases.removeRedundantAliases.match15") =>
//            Rule.coverage += "RemoveRedundantAliases.removeRedundantAliases.match15"
//            removeRedundantAlias(_, excluded)
//          case _: Window if sentinel(true, "RemoveRedundantAliases.removeRedundantAliases.match16") =>
//            Rule.coverage += "RemoveRedundantAliases.removeRedundantAliases.match16"
//            removeRedundantAlias(_, excluded)
//          case _ if sentinel(true, "RemoveRedundantAliases.removeRedundantAliases.match17") =>
//            Rule.coverage += "RemoveRedundantAliases.removeRedundantAliases.match17"
//            identity[Expression]
//        }
//        newNode.mapExpressions {
//          expr => clean(expr.transform {
//            case a: Attribute if sentinel(true, "RemoveRedundantAliases.removeRedundantAliases.case18") =>
//              Rule.coverage += "RemoveRedundantAliases.removeRedundantAliases.case18"
//              mapping.get(a).map(_.withName(a.name)).getOrElse(a)
//          })
//        }
//    }
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = removeRedundantAliases(plan, AttributeSet.empty)
//}
//object RemoveNoopOperators extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(_.containsAnyPattern(PROJECT, WINDOW), ruleId) {
//    case p @ Project(projectList, child) if sentinel(true, "RemoveNoopOperators.apply.case1") && child.sameOutput(p) =>
//      Rule.coverage += "RemoveNoopOperators.apply.case1"
//      val newChild = child match {
//        case p: Project if sentinel(true, "RemoveNoopOperators.apply.match19") =>
//          Rule.coverage += "RemoveNoopOperators.apply.match19"
//          p.copy(projectList = restoreOriginalOutputNames(p.projectList, projectList.map(_.name)))
//        case agg: Aggregate if sentinel(true, "RemoveNoopOperators.apply.match20") =>
//          Rule.coverage += "RemoveNoopOperators.apply.match20"
//          agg.copy(aggregateExpressions = restoreOriginalOutputNames(agg.aggregateExpressions, projectList.map(_.name)))
//        case _ if sentinel(true, "RemoveNoopOperators.apply.match21") =>
//          Rule.coverage += "RemoveNoopOperators.apply.match21"
//          child
//      }
//      if (sentinel(true, "RemoveNoopOperators.apply.then22") && newChild.output.zip(projectList).forall {
//        case (a1, a2) if sentinel(true, "RemoveNoopOperators.apply.case24") =>
//          Rule.coverage += "RemoveNoopOperators.apply.case24"
//          a1.name == a2.name
//      }) {
//        Rule.coverage += "RemoveNoopOperators.apply.then22"
//        newChild
//      } else {
//        Rule.coverage += "RemoveNoopOperators.apply.else23"
//        sentinel(true, "RemoveNoopOperators.apply.else23")
//        p
//      }
//    case w: Window if sentinel(true, "RemoveNoopOperators.apply.case2") && w.windowExpressions.isEmpty =>
//      Rule.coverage += "RemoveNoopOperators.apply.case2"
//      w.child
//  }
//}
//object RemoveNoopUnion extends Rule[LogicalPlan] {
//  private def removeAliasOnlyProject(plan: LogicalPlan): LogicalPlan = plan match {
//    case p @ Project(projectList, child) if sentinel(true, "RemoveNoopUnion.removeAliasOnlyProject.match1") =>
//      Rule.coverage += "RemoveNoopUnion.removeAliasOnlyProject.match1"
//      val aliasOnly = projectList.length == child.output.length && projectList.zip(child.output).forall {
//        case (Alias(left: Attribute, _), right) if sentinel(true, "RemoveNoopUnion.removeAliasOnlyProject.case25") =>
//          Rule.coverage += "RemoveNoopUnion.removeAliasOnlyProject.case25"
//          left.semanticEquals(right)
//        case (left: Attribute, right) if sentinel(true, "RemoveNoopUnion.removeAliasOnlyProject.case26") =>
//          Rule.coverage += "RemoveNoopUnion.removeAliasOnlyProject.case26"
//          left.semanticEquals(right)
//        case _ if sentinel(true, "RemoveNoopUnion.removeAliasOnlyProject.case27") =>
//          Rule.coverage += "RemoveNoopUnion.removeAliasOnlyProject.case27"
//          false
//      }
//      if (sentinel(true, "RemoveNoopUnion.removeAliasOnlyProject.then28") && aliasOnly) {
//        Rule.coverage += "RemoveNoopUnion.removeAliasOnlyProject.then28"
//        child
//      } else {
//        Rule.coverage += "RemoveNoopUnion.removeAliasOnlyProject.else29"
//        sentinel(true, "RemoveNoopUnion.removeAliasOnlyProject.else29")
//        p
//      }
//    case _ if sentinel(true, "RemoveNoopUnion.removeAliasOnlyProject.match2") =>
//      Rule.coverage += "RemoveNoopUnion.removeAliasOnlyProject.match2"
//      plan
//  }
//  private def simplifyUnion(u: Union): LogicalPlan = {
//    val uniqueChildren = mutable.ArrayBuffer.empty[LogicalPlan]
//    val uniqueChildrenKey = mutable.HashSet.empty[LogicalPlan]
//    u.children.foreach { c =>
//      val key = removeAliasOnlyProject(c).canonicalized
//      if (sentinel(true, "RemoveNoopUnion.simplifyUnion.then1") && !uniqueChildrenKey.contains(key)) {
//        Rule.coverage += "RemoveNoopUnion.simplifyUnion.then1"
//        uniqueChildren += c
//        uniqueChildrenKey += key
//      } else {
//        Rule.coverage += "RemoveNoopUnion.simplifyUnion.else2"
//        sentinel(true, "RemoveNoopUnion.simplifyUnion.else2")
//      }
//    }
//    if (sentinel(true, "RemoveNoopUnion.simplifyUnion.then3") && uniqueChildren.size == 1) {
//      Rule.coverage += "RemoveNoopUnion.simplifyUnion.then3"
//      u.children.head
//    } else {
//      Rule.coverage += "RemoveNoopUnion.simplifyUnion.else4"
//      sentinel(true, "RemoveNoopUnion.simplifyUnion.else4")
//      u.copy(children = uniqueChildren.toSeq)
//    }
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(_.containsAllPatterns(DISTINCT_LIKE, UNION)) {
//    case d @ Distinct(u: Union) if sentinel(true, "RemoveNoopUnion.apply.case1") =>
//      Rule.coverage += "RemoveNoopUnion.apply.case1"
//      d.withNewChildren(Seq(simplifyUnion(u)))
//    case d @ Deduplicate(_, u: Union) if sentinel(true, "RemoveNoopUnion.apply.case2") =>
//      Rule.coverage += "RemoveNoopUnion.apply.case2"
//      d.withNewChildren(Seq(simplifyUnion(u)))
//    case d @ DeduplicateWithinWatermark(_, u: Union) if sentinel(true, "RemoveNoopUnion.apply.case3") =>
//      Rule.coverage += "RemoveNoopUnion.apply.case3"
//      d.withNewChildren(Seq(simplifyUnion(u)))
//  }
//}
//object LimitPushDown extends Rule[LogicalPlan] {
//  private def stripGlobalLimitIfPresent(plan: LogicalPlan): LogicalPlan = {
//    plan match {
//      case GlobalLimit(_, child) if sentinel(true, "LimitPushDown.stripGlobalLimitIfPresent.match1") =>
//        Rule.coverage += "LimitPushDown.stripGlobalLimitIfPresent.match1"
//        child
//      case _ if sentinel(true, "LimitPushDown.stripGlobalLimitIfPresent.match2") =>
//        Rule.coverage += "LimitPushDown.stripGlobalLimitIfPresent.match2"
//        plan
//    }
//  }
//  private def maybePushLocalLimit(limitExp: Expression, plan: LogicalPlan): LogicalPlan = {
//    (limitExp, plan.maxRowsPerPartition) match {
//      case (IntegerLiteral(newLimit), Some(childMaxRows)) if sentinel(true, "LimitPushDown.maybePushLocalLimit.match1") && newLimit < childMaxRows =>
//        Rule.coverage += "LimitPushDown.maybePushLocalLimit.match1"
//        LocalLimit(limitExp, stripGlobalLimitIfPresent(plan))
//      case (_, None) if sentinel(true, "LimitPushDown.maybePushLocalLimit.match2") =>
//        Rule.coverage += "LimitPushDown.maybePushLocalLimit.match2"
//        LocalLimit(limitExp, stripGlobalLimitIfPresent(plan))
//      case _ if sentinel(true, "LimitPushDown.maybePushLocalLimit.match3") =>
//        Rule.coverage += "LimitPushDown.maybePushLocalLimit.match3"
//        plan
//    }
//  }
//  private def pushLocalLimitThroughJoin(limitExpr: Expression, join: Join): Join = {
//    join.joinType match {
//      case RightOuter if sentinel(true, "LimitPushDown.pushLocalLimitThroughJoin.match1") && join.condition.nonEmpty =>
//        Rule.coverage += "LimitPushDown.pushLocalLimitThroughJoin.match1"
//        join.copy(right = maybePushLocalLimit(limitExpr, join.right))
//      case LeftOuter if sentinel(true, "LimitPushDown.pushLocalLimitThroughJoin.match2") && join.condition.nonEmpty =>
//        Rule.coverage += "LimitPushDown.pushLocalLimitThroughJoin.match2"
//        join.copy(left = maybePushLocalLimit(limitExpr, join.left))
//      case _: InnerLike | RightOuter | LeftOuter | FullOuter if sentinel(true, "LimitPushDown.pushLocalLimitThroughJoin.match3") && join.condition.isEmpty =>
//        Rule.coverage += "LimitPushDown.pushLocalLimitThroughJoin.match3"
//        join.copy(left = maybePushLocalLimit(limitExpr, join.left), right = maybePushLocalLimit(limitExpr, join.right))
//      case LeftSemi | LeftAnti if sentinel(true, "LimitPushDown.pushLocalLimitThroughJoin.match4") && join.condition.isEmpty =>
//        Rule.coverage += "LimitPushDown.pushLocalLimitThroughJoin.match4"
//        join.copy(left = maybePushLocalLimit(limitExpr, join.left))
//      case _ if sentinel(true, "LimitPushDown.pushLocalLimitThroughJoin.match5") =>
//        Rule.coverage += "LimitPushDown.pushLocalLimitThroughJoin.match5"
//        join
//    }
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsAnyPattern(LIMIT, LEFT_SEMI_OR_ANTI_JOIN), ruleId) {
//    case LocalLimit(exp, u: Union) if sentinel(true, "LimitPushDown.apply.case1") =>
//      Rule.coverage += "LimitPushDown.apply.case1"
//      LocalLimit(exp, u.copy(children = u.children.map(maybePushLocalLimit(exp, _))))
//    case LocalLimit(exp, join: Join) if sentinel(true, "LimitPushDown.apply.case2") =>
//      Rule.coverage += "LimitPushDown.apply.case2"
//      LocalLimit(exp, pushLocalLimitThroughJoin(exp, join))
//    case LocalLimit(exp, project @ Project(_, join: Join)) if sentinel(true, "LimitPushDown.apply.case3") =>
//      Rule.coverage += "LimitPushDown.apply.case3"
//      LocalLimit(exp, project.copy(child = pushLocalLimitThroughJoin(exp, join)))
//    case Limit(le @ IntegerLiteral(1), a: Aggregate) if sentinel(true, "LimitPushDown.apply.case4") && a.groupOnly =>
//      Rule.coverage += "LimitPushDown.apply.case4"
//      Limit(le, Project(a.aggregateExpressions, LocalLimit(le, a.child)))
//    case Limit(le @ IntegerLiteral(1), p @ Project(_, a: Aggregate)) if sentinel(true, "LimitPushDown.apply.case5") && a.groupOnly =>
//      Rule.coverage += "LimitPushDown.apply.case5"
//      Limit(le, p.copy(child = Project(a.aggregateExpressions, LocalLimit(le, a.child))))
//    case LocalLimit(le, Offset(oe, grandChild)) if sentinel(true, "LimitPushDown.apply.case6") =>
//      Rule.coverage += "LimitPushDown.apply.case6"
//      Offset(oe, LocalLimit(Add(le, oe), grandChild))
//    case j @ Join(_, right, LeftSemiOrAnti(_), None, _) if sentinel(true, "LimitPushDown.apply.case7") && !right.maxRows.exists(_ <= 1) =>
//      Rule.coverage += "LimitPushDown.apply.case7"
//      j.copy(right = maybePushLocalLimit(Literal(1, IntegerType), right))
//    case LocalLimit(le, udf: BatchEvalPython) if sentinel(true, "LimitPushDown.apply.case8") =>
//      Rule.coverage += "LimitPushDown.apply.case8"
//      LocalLimit(le, udf.copy(child = maybePushLocalLimit(le, udf.child)))
//    case LocalLimit(le, p @ Project(_, udf: BatchEvalPython)) if sentinel(true, "LimitPushDown.apply.case9") =>
//      Rule.coverage += "LimitPushDown.apply.case9"
//      LocalLimit(le, p.copy(child = udf.copy(child = maybePushLocalLimit(le, udf.child))))
//    case LocalLimit(le, udf: ArrowEvalPython) if sentinel(true, "LimitPushDown.apply.case10") =>
//      Rule.coverage += "LimitPushDown.apply.case10"
//      LocalLimit(le, udf.copy(child = maybePushLocalLimit(le, udf.child)))
//    case LocalLimit(le, p @ Project(_, udf: ArrowEvalPython)) if sentinel(true, "LimitPushDown.apply.case11") =>
//      Rule.coverage += "LimitPushDown.apply.case11"
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
//      case a: Attribute if sentinel(true, "PushProjectionThroughUnion.pushToRight.case30") =>
//        Rule.coverage += "PushProjectionThroughUnion.pushToRight.case30"
//        rewrites(a)
//    } match {
//      case Alias(child, alias) if sentinel(true, "PushProjectionThroughUnion.pushToRight.match1") =>
//        Rule.coverage += "PushProjectionThroughUnion.pushToRight.match1"
//        Alias(child, alias)()
//      case other if sentinel(true, "PushProjectionThroughUnion.pushToRight.match2") =>
//        Rule.coverage += "PushProjectionThroughUnion.pushToRight.match2"
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
//    case Project(projectList, u: Union) if sentinel(true, "PushProjectionThroughUnion.apply.case1") && (projectList.forall(_.deterministic) && u.children.nonEmpty) =>
//      Rule.coverage += "PushProjectionThroughUnion.apply.case1"
//      u.copy(children = pushProjectionThroughUnion(projectList, u))
//  }
//}
//object ColumnPruning extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = removeProjectBeforeFilter(plan.transformWithPruning(AlwaysProcess.fn, ruleId) {
//    case p @ Project(_, p2: Project) if sentinel(true, "ColumnPruning.apply.case1") && !p2.outputSet.subsetOf(p.references) =>
//      Rule.coverage += "ColumnPruning.apply.case1"
//      p.copy(child = p2.copy(projectList = p2.projectList.filter(p.references.contains)))
//    case p @ Project(_, a: Aggregate) if sentinel(true, "ColumnPruning.apply.case2") && !a.outputSet.subsetOf(p.references) =>
//      Rule.coverage += "ColumnPruning.apply.case2"
//      p.copy(child = a.copy(aggregateExpressions = a.aggregateExpressions.filter(p.references.contains)))
//    case a @ Project(_, e @ Expand(_, _, grandChild)) if sentinel(true, "ColumnPruning.apply.case3") && !e.outputSet.subsetOf(a.references) =>
//      Rule.coverage += "ColumnPruning.apply.case3"
//      val newOutput = e.output.filter(a.references.contains(_))
//      val newProjects = e.projections.map {
//        proj => proj.zip(e.output).filter {
//          case (_, a) if sentinel(true, "ColumnPruning.apply.case31") =>
//            Rule.coverage += "ColumnPruning.apply.case31"
//            newOutput.contains(a)
//        }.map(_._1)
//      }
//      a.copy(child = Expand(newProjects, newOutput, grandChild))
//    case p @ Project(_, a @ AttachDistributedSequence(_, grandChild)) if sentinel(true, "ColumnPruning.apply.case4") && !p.references.contains(a.sequenceAttr) =>
//      Rule.coverage += "ColumnPruning.apply.case4"
//      p.copy(child = prunedChild(grandChild, p.references))
//    case d @ DeserializeToObject(_, _, child) if sentinel(true, "ColumnPruning.apply.case5") && !child.outputSet.subsetOf(d.references) =>
//      Rule.coverage += "ColumnPruning.apply.case5"
//      d.copy(child = prunedChild(child, d.references))
//    case a @ Aggregate(_, _, child) if sentinel(true, "ColumnPruning.apply.case6") && !child.outputSet.subsetOf(a.references) =>
//      Rule.coverage += "ColumnPruning.apply.case6"
//      a.copy(child = prunedChild(child, a.references))
//    case f @ FlatMapGroupsInPandas(_, _, _, child) if sentinel(true, "ColumnPruning.apply.case7") && !child.outputSet.subsetOf(f.references) =>
//      Rule.coverage += "ColumnPruning.apply.case7"
//      f.copy(child = prunedChild(child, f.references))
//    case e @ Expand(_, _, child) if sentinel(true, "ColumnPruning.apply.case8") && !child.outputSet.subsetOf(e.references) =>
//      Rule.coverage += "ColumnPruning.apply.case8"
//      e.copy(child = prunedChild(child, e.references))
//    case e @ MergeRows(_, _, _, _, _, _, _, child) if sentinel(true, "ColumnPruning.apply.case9") && !child.outputSet.subsetOf(e.references) =>
//      Rule.coverage += "ColumnPruning.apply.case9"
//      e.copy(child = prunedChild(child, e.references))
//    case p @ Project(_, g: Generate) if sentinel(true, "ColumnPruning.apply.case10") && p.references != g.outputSet =>
//      Rule.coverage += "ColumnPruning.apply.case10"
//      val requiredAttrs = p.references -- g.producedAttributes ++ g.generator.references
//      val newChild = prunedChild(g.child, requiredAttrs)
//      val unrequired = g.generator.references -- p.references
//      val unrequiredIndices = newChild.output.zipWithIndex.filter(t => unrequired.contains(t._1)).map(_._2)
//      p.copy(child = g.copy(child = newChild, unrequiredChildIndex = unrequiredIndices))
//    case GeneratorNestedColumnAliasing(rewrittenPlan) if sentinel(true, "ColumnPruning.apply.case11") =>
//      Rule.coverage += "ColumnPruning.apply.case11"
//      rewrittenPlan
//    case j @ Join(_, right, LeftExistence(_), _, _) if sentinel(true, "ColumnPruning.apply.case12") =>
//      Rule.coverage += "ColumnPruning.apply.case12"
//      j.copy(right = prunedChild(right, j.references))
//    case p @ Project(_, _: SetOperation) if sentinel(true, "ColumnPruning.apply.case13") =>
//      Rule.coverage += "ColumnPruning.apply.case13"
//      p
//    case p @ Project(_, _: Distinct) if sentinel(true, "ColumnPruning.apply.case14") =>
//      Rule.coverage += "ColumnPruning.apply.case14"
//      p
//    case p @ Project(_, u: Union) if sentinel(true, "ColumnPruning.apply.case15") =>
//      Rule.coverage += "ColumnPruning.apply.case15"
//      if (sentinel(true, "ColumnPruning.apply.then32") && !u.outputSet.subsetOf(p.references)) {
//        Rule.coverage += "ColumnPruning.apply.then32"
//        val firstChild = u.children.head
//        val newOutput = prunedChild(firstChild, p.references).output
//        val newChildren = u.children.map { p =>
//          val selected = p.output.zipWithIndex.filter {
//            case (a, i) if sentinel(true, "ColumnPruning.apply.case34") =>
//              Rule.coverage += "ColumnPruning.apply.case34"
//              newOutput.contains(firstChild.output(i))
//          }.map(_._1)
//          Project(selected, p)
//        }
//        p.copy(child = u.withNewChildren(newChildren))
//      } else {
//        Rule.coverage += "ColumnPruning.apply.else33"
//        sentinel(true, "ColumnPruning.apply.else33")
//        p
//      }
//    case p @ Project(_, w: Window) if sentinel(true, "ColumnPruning.apply.case16") && !w.windowOutputSet.subsetOf(p.references) =>
//      Rule.coverage += "ColumnPruning.apply.case16"
//      val windowExprs = w.windowExpressions.filter(p.references.contains)
//      val newChild = if (sentinel(true, "ColumnPruning.apply.then35") && windowExprs.isEmpty) {
//        Rule.coverage += "ColumnPruning.apply.then35"
//        w.child
//      } else {
//        Rule.coverage += "ColumnPruning.apply.else36"
//        sentinel(true, "ColumnPruning.apply.else36")
//        w.copy(windowExpressions = windowExprs)
//      }
//      p.copy(child = newChild)
//    case p @ Project(_, w: WithCTE) if sentinel(true, "ColumnPruning.apply.case17") =>
//      Rule.coverage += "ColumnPruning.apply.case17"
//      if (sentinel(true, "ColumnPruning.apply.then37") && !w.outputSet.subsetOf(p.references)) {
//        Rule.coverage += "ColumnPruning.apply.then37"
//        p.copy(child = w.withNewPlan(prunedChild(w.plan, p.references)))
//      } else {
//        Rule.coverage += "ColumnPruning.apply.else38"
//        sentinel(true, "ColumnPruning.apply.else38")
//        p
//      }
//    case p @ Project(_, _: LeafNode) if sentinel(true, "ColumnPruning.apply.case18") =>
//      Rule.coverage += "ColumnPruning.apply.case18"
//      p
//    case NestedColumnAliasing(rewrittenPlan) if sentinel(true, "ColumnPruning.apply.case19") =>
//      Rule.coverage += "ColumnPruning.apply.case19"
//      rewrittenPlan
//    case p @ Project(_, child) if sentinel(true, "ColumnPruning.apply.case20") && !child.isInstanceOf[Project] =>
//      Rule.coverage += "ColumnPruning.apply.case20"
//      val required = child.references ++ p.references
//      if (sentinel(true, "ColumnPruning.apply.then39") && !child.inputSet.subsetOf(required)) {
//        Rule.coverage += "ColumnPruning.apply.then39"
//        val newChildren = child.children.map(c => prunedChild(c, required))
//        p.copy(child = child.withNewChildren(newChildren))
//      } else {
//        Rule.coverage += "ColumnPruning.apply.else40"
//        sentinel(true, "ColumnPruning.apply.else40")
//        p
//      }
//  })
//  private def prunedChild(c: LogicalPlan, allReferences: AttributeSet) = if (sentinel(true, "ColumnPruning.prunedChild.then1") && !c.outputSet.subsetOf(allReferences)) {
//    Rule.coverage += "ColumnPruning.prunedChild.then1"
//    Project(c.output.filter(allReferences.contains), c)
//  } else {
//    Rule.coverage += "ColumnPruning.prunedChild.else2"
//    sentinel(true, "ColumnPruning.prunedChild.else2")
//    c
//  }
//  private def removeProjectBeforeFilter(plan: LogicalPlan): LogicalPlan = plan transformUp {
//    case p1 @ Project(_, f @ Filter(e, p2 @ Project(_, child))) if sentinel(true, "ColumnPruning.removeProjectBeforeFilter.case1") && (p2.outputSet.subsetOf(child.outputSet) && p2.projectList.forall(_.isInstanceOf[AttributeReference]) && !hasConflictingAttrsWithSubquery(e, child)) =>
//      Rule.coverage += "ColumnPruning.removeProjectBeforeFilter.case1"
//      p1.copy(child = f.copy(child = child))
//  }
//  private def hasConflictingAttrsWithSubquery(predicate: Expression, child: LogicalPlan): Boolean = {
//    predicate.find {
//      case s: SubqueryExpression if sentinel(true, "ColumnPruning.hasConflictingAttrsWithSubquery.case1") && s.plan.outputSet.intersect(child.outputSet).nonEmpty =>
//        Rule.coverage += "ColumnPruning.hasConflictingAttrsWithSubquery.case1"
//        true
//      case _ if sentinel(true, "ColumnPruning.hasConflictingAttrsWithSubquery.case2") =>
//        Rule.coverage += "ColumnPruning.hasConflictingAttrsWithSubquery.case2"
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
//      case p1 @ Project(_, p2: Project) if sentinel(true, "CollapseProject.apply.case1") && canCollapseExpressions(p1.projectList, p2.projectList, alwaysInline) =>
//        Rule.coverage += "CollapseProject.apply.case1"
//        p2.copy(projectList = buildCleanedProjectList(p1.projectList, p2.projectList))
//      case p @ Project(_, agg: Aggregate) if sentinel(true, "CollapseProject.apply.case2") && (canCollapseExpressions(p.projectList, agg.aggregateExpressions, alwaysInline) && canCollapseAggregate(p, agg)) =>
//        Rule.coverage += "CollapseProject.apply.case2"
//        agg.copy(aggregateExpressions = buildCleanedProjectList(p.projectList, agg.aggregateExpressions))
//      case Project(l1, g @ GlobalLimit(_, limit @ LocalLimit(_, p2 @ Project(l2, _)))) if sentinel(true, "CollapseProject.apply.case3") && isRenaming(l1, l2) =>
//        Rule.coverage += "CollapseProject.apply.case3"
//        val newProjectList = buildCleanedProjectList(l1, l2)
//        g.copy(child = limit.copy(child = p2.copy(projectList = newProjectList)))
//      case Project(l1, limit @ LocalLimit(_, p2 @ Project(l2, _))) if sentinel(true, "CollapseProject.apply.case4") && isRenaming(l1, l2) =>
//        Rule.coverage += "CollapseProject.apply.case4"
//        val newProjectList = buildCleanedProjectList(l1, l2)
//        limit.copy(child = p2.copy(projectList = newProjectList))
//      case Project(l1, r @ Repartition(_, _, p @ Project(l2, _))) if sentinel(true, "CollapseProject.apply.case5") && isRenaming(l1, l2) =>
//        Rule.coverage += "CollapseProject.apply.case5"
//        r.copy(child = p.copy(projectList = buildCleanedProjectList(l1, p.projectList)))
//      case Project(l1, s @ Sample(_, _, _, _, p2 @ Project(l2, _))) if sentinel(true, "CollapseProject.apply.case6") && isRenaming(l1, l2) =>
//        Rule.coverage += "CollapseProject.apply.case6"
//        s.copy(child = p2.copy(projectList = buildCleanedProjectList(l1, p2.projectList)))
//    }
//  }
//  def canCollapseExpressions(consumers: Seq[Expression], producers: Seq[NamedExpression], alwaysInline: Boolean): Boolean = {
//    canCollapseExpressions(consumers, getAliasMap(producers), alwaysInline)
//  }
//  def canCollapseExpressions(consumers: Seq[Expression], producerMap: Map[Attribute, Expression], alwaysInline: Boolean = false): Boolean = {
//    consumers.filter(_.references.exists(producerMap.contains)).flatMap(collectReferences).groupBy(identity).mapValues(_.size).forall {
//      case (reference, count) if sentinel(true, "CollapseProject.canCollapseExpressions.case1") =>
//        Rule.coverage += "CollapseProject.canCollapseExpressions.case1"
//        val producer = producerMap.getOrElse(reference, reference)
//        val relatedConsumers = consumers.filter(_.references.contains(reference))
//        def cheapToInlineProducer: Boolean = trimAliases(producer) match {
//          case e @ (_: CreateNamedStruct | _: UpdateFields | _: CreateMap | _: CreateArray) if sentinel(true, "CollapseProject.cheapToInlineProducer.match1") =>
//            Rule.coverage += "CollapseProject.cheapToInlineProducer.match1"
//            var nonCheapAccessSeen = false
//            def nonCheapAccessVisitor(): Boolean = {
//              try {
//                nonCheapAccessSeen
//              } finally {
//                nonCheapAccessSeen = true
//              }
//            }
//            !relatedConsumers.exists(findNonCheapAccesses(_, reference, e, nonCheapAccessVisitor))
//          case other if sentinel(true, "CollapseProject.cheapToInlineProducer.match2") =>
//            Rule.coverage += "CollapseProject.cheapToInlineProducer.match2"
//            isCheap(other)
//        }
//        producer.deterministic && (count == 1 || alwaysInline || cheapToInlineProducer)
//    }
//  }
//  private object ExtractOnlyRef {
//    @scala.annotation.tailrec def unapply(expr: Expression): Option[Attribute] = expr match {
//      case a: Alias if sentinel(true, "ExtractOnlyRef.unapply.match1") =>
//        Rule.coverage += "ExtractOnlyRef.unapply.match1"
//        unapply(a.child)
//      case e: ExtractValue if sentinel(true, "ExtractOnlyRef.unapply.match2") =>
//        Rule.coverage += "ExtractOnlyRef.unapply.match2"
//        unapply(e.children.head)
//      case a: Attribute if sentinel(true, "ExtractOnlyRef.unapply.match3") =>
//        Rule.coverage += "ExtractOnlyRef.unapply.match3"
//        Some(a)
//      case _ if sentinel(true, "ExtractOnlyRef.unapply.match4") =>
//        Rule.coverage += "ExtractOnlyRef.unapply.match4"
//        None
//    }
//  }
//  private def inlineReference(expr: Expression, ref: Attribute, refExpr: Expression): Expression = {
//    expr.transformUp {
//      case a: Attribute if sentinel(true, "CollapseProject.inlineReference.case1") && a.semanticEquals(ref) =>
//        Rule.coverage += "CollapseProject.inlineReference.case1"
//        refExpr
//    }
//  }
//  private object SimplifyExtractValueExecutor extends RuleExecutor[LogicalPlan] { override val batches = Batch("SimplifyExtractValueOps", FixedPoint(10), SimplifyExtractValueOps, ConstantFolding, SimplifyConditionals) :: Nil }
//  private def simplifyExtractValues(expr: Expression): Expression = {
//    val fakePlan = Project(Seq(Alias(expr, "fake")()), LocalRelation(Nil))
//    SimplifyExtractValueExecutor.execute(fakePlan).asInstanceOf[Project].projectList.head.asInstanceOf[Alias].child
//  }
//  private def findNonCheapAccesses(consumer: Expression, ref: Attribute, refExpr: Expression, nonCheapAccessVisitor: () => Boolean): Boolean = consumer match {
//    case attr: Attribute if sentinel(true, "CollapseProject.findNonCheapAccesses.match1") && attr.semanticEquals(ref) =>
//      Rule.coverage += "CollapseProject.findNonCheapAccesses.match1"
//      nonCheapAccessVisitor()
//    case e @ ExtractOnlyRef(attr) if sentinel(true, "CollapseProject.findNonCheapAccesses.match2") && attr.semanticEquals(ref) =>
//      Rule.coverage += "CollapseProject.findNonCheapAccesses.match2"
//      val finalExpr = simplifyExtractValues(inlineReference(e, ref, refExpr))
//      !isCheap(finalExpr) && nonCheapAccessVisitor()
//    case _ if sentinel(true, "CollapseProject.findNonCheapAccesses.match3") =>
//      Rule.coverage += "CollapseProject.findNonCheapAccesses.match3"
//      consumer.children.exists(findNonCheapAccesses(_, ref, refExpr, nonCheapAccessVisitor))
//  }
//  private def canCollapseAggregate(p: Project, a: Aggregate): Boolean = {
//    p.projectList.forall(_.collect {
//      case s: ScalarSubquery if sentinel(true, "CollapseProject.canCollapseAggregate.case1") && s.outerAttrs.nonEmpty =>
//        Rule.coverage += "CollapseProject.canCollapseAggregate.case1"
//        s
//    }.isEmpty)
//  }
//  def buildCleanedProjectList(upper: Seq[NamedExpression], lower: Seq[NamedExpression]): Seq[NamedExpression] = {
//    val aliases = getAliasMap(lower)
//    upper.map(replaceAliasButKeepName(_, aliases))
//  }
//  def isCheap(e: Expression): Boolean = e match {
//    case _: Attribute | _: OuterReference if sentinel(true, "CollapseProject.isCheap.match1") =>
//      Rule.coverage += "CollapseProject.isCheap.match1"
//      true
//    case _ if sentinel(true, "CollapseProject.isCheap.match2") && e.foldable =>
//      Rule.coverage += "CollapseProject.isCheap.match2"
//      true
//    case _: PythonUDF if sentinel(true, "CollapseProject.isCheap.match3") =>
//      Rule.coverage += "CollapseProject.isCheap.match3"
//      true
//    case _: Alias | _: ExtractValue if sentinel(true, "CollapseProject.isCheap.match4") =>
//      Rule.coverage += "CollapseProject.isCheap.match4"
//      e.children.forall(isCheap)
//    case _ if sentinel(true, "CollapseProject.isCheap.match5") =>
//      Rule.coverage += "CollapseProject.isCheap.match5"
//      false
//  }
//  private def collectReferences(e: Expression): Seq[Attribute] = e.collect {
//    case a: Attribute if sentinel(true, "CollapseProject.collectReferences.case1") =>
//      Rule.coverage += "CollapseProject.collectReferences.case1"
//      a
//  }
//  private def isRenaming(list1: Seq[NamedExpression], list2: Seq[NamedExpression]): Boolean = {
//    list1.length == list2.length && list1.zip(list2).forall {
//      case (e1, e2) if sentinel(true, "CollapseProject.isRenaming.case1") && e1.semanticEquals(e2) =>
//        Rule.coverage += "CollapseProject.isRenaming.case1"
//        true
//      case (Alias(a: Attribute, _), b) if sentinel(true, "CollapseProject.isRenaming.case2") && (a.metadata == Metadata.empty && a.name == b.name) =>
//        Rule.coverage += "CollapseProject.isRenaming.case2"
//        true
//      case _ if sentinel(true, "CollapseProject.isRenaming.case3") =>
//        Rule.coverage += "CollapseProject.isRenaming.case3"
//        false
//    }
//  }
//}
//object CollapseRepartition extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(_.containsAnyPattern(REPARTITION_OPERATION, REBALANCE_PARTITIONS), ruleId) {
//    case r @ Repartition(_, _, child: RepartitionOperation) if sentinel(true, "CollapseRepartition.apply.case1") =>
//      Rule.coverage += "CollapseRepartition.apply.case1"
//      (r.shuffle, child.shuffle) match {
//        case (false, true) if sentinel(true, "CollapseRepartition.apply.match41") =>
//          Rule.coverage += "CollapseRepartition.apply.match41"
//          if (sentinel(true, "CollapseRepartition.apply.then43") && r.numPartitions >= child.numPartitions) {
//            Rule.coverage += "CollapseRepartition.apply.then43"
//            child
//          } else {
//            Rule.coverage += "CollapseRepartition.apply.else44"
//            sentinel(true, "CollapseRepartition.apply.else44")
//            r
//          }
//        case _ if sentinel(true, "CollapseRepartition.apply.match42") =>
//          Rule.coverage += "CollapseRepartition.apply.match42"
//          r.copy(child = child.child)
//      }
//    case r @ RepartitionByExpression(_, child @ (Sort(_, true, _) | _: RepartitionOperation), _, _) if sentinel(true, "CollapseRepartition.apply.case2") =>
//      Rule.coverage += "CollapseRepartition.apply.case2"
//      r.withNewChildren(child.children)
//    case r @ RebalancePartitions(_, child @ (_: Sort | _: RepartitionOperation), _, _) if sentinel(true, "CollapseRepartition.apply.case3") =>
//      Rule.coverage += "CollapseRepartition.apply.case3"
//      r.withNewChildren(child.children)
//    case r @ RebalancePartitions(_, child: RebalancePartitions, _, _) if sentinel(true, "CollapseRepartition.apply.case4") =>
//      Rule.coverage += "CollapseRepartition.apply.case4"
//      r.withNewChildren(child.children)
//  }
//}
//object OptimizeRepartition extends Rule[LogicalPlan] {
//  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(REPARTITION_OPERATION), ruleId) {
//    case r @ RepartitionByExpression(partitionExpressions, _, numPartitions, _) if sentinel(true, "OptimizeRepartition.apply.case1") && (partitionExpressions.nonEmpty && partitionExpressions.forall(_.foldable) && numPartitions.isEmpty) =>
//      Rule.coverage += "OptimizeRepartition.apply.case1"
//      r.copy(optNumPartitions = Some(1))
//  }
//}
//object OptimizeWindowFunctions extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveExpressionsWithPruning(_.containsPattern(WINDOW_EXPRESSION), ruleId) {
//    case we @ WindowExpression(AggregateExpression(first: First, _, _, _, _), WindowSpecDefinition(_, orderSpec, frameSpecification: SpecifiedWindowFrame)) if sentinel(true, "OptimizeWindowFunctions.apply.case1") && (orderSpec.nonEmpty && frameSpecification.frameType == RowFrame && frameSpecification.lower == UnboundedPreceding && (frameSpecification.upper == UnboundedFollowing || frameSpecification.upper == CurrentRow)) =>
//      Rule.coverage += "OptimizeWindowFunctions.apply.case1"
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
//    case w1 @ Window(we1, _, _, w2 @ Window(we2, _, _, grandChild)) if sentinel(true, "CollapseWindow.apply.case1") && windowsCompatible(w1, w2) =>
//      Rule.coverage += "CollapseWindow.apply.case1"
//      w1.copy(windowExpressions = we2 ++ we1, child = grandChild)
//    case w1 @ Window(we1, _, _, Project(pl, w2 @ Window(we2, _, _, grandChild))) if sentinel(true, "CollapseWindow.apply.case2") && (windowsCompatible(w1, w2) && w1.references.subsetOf(grandChild.outputSet)) =>
//      Rule.coverage += "CollapseWindow.apply.case2"
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
//    case w1 @ Window(_, _, _, w2 @ Window(_, _, _, grandChild)) if sentinel(true, "TransposeWindow.apply.case1") && windowsCompatible(w1, w2) =>
//      Rule.coverage += "TransposeWindow.apply.case1"
//      Project(w1.output, w2.copy(child = w1.copy(child = grandChild)))
//    case w1 @ Window(_, _, _, Project(pl, w2 @ Window(_, _, _, grandChild))) if sentinel(true, "TransposeWindow.apply.case2") && (windowsCompatible(w1, w2) && w1.references.subsetOf(grandChild.outputSet)) =>
//      Rule.coverage += "TransposeWindow.apply.case2"
//      Project(pl ++ w1.windowOutputSet, w2.copy(child = w1.copy(child = grandChild)))
//  }
//}
//object InferFiltersFromGenerate extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(_.containsPattern(GENERATE)) {
//    case generate @ Generate(g, _, false, _, _, _) if sentinel(true, "InferFiltersFromGenerate.apply.case1") && canInferFilters(g) =>
//      Rule.coverage += "InferFiltersFromGenerate.apply.case1"
//      assert(g.children.length == 1)
//      val input = g.children.head
//      if (sentinel(true, "InferFiltersFromGenerate.apply.then45") && (input.isInstanceOf[Attribute])) {
//        Rule.coverage += "InferFiltersFromGenerate.apply.then45"
//        val inferredFilters = ExpressionSet(Seq(GreaterThan(Size(input), Literal(0)), IsNotNull(input))) -- generate.child.constraints
//        if (sentinel(true, "InferFiltersFromGenerate.apply.then47") && inferredFilters.nonEmpty) {
//          Rule.coverage += "InferFiltersFromGenerate.apply.then47"
//          generate.copy(child = Filter(inferredFilters.reduce(And), generate.child))
//        } else {
//          Rule.coverage += "InferFiltersFromGenerate.apply.else48"
//          sentinel(true, "InferFiltersFromGenerate.apply.else48")
//          generate
//        }
//      } else {
//        Rule.coverage += "InferFiltersFromGenerate.apply.else46"
//        sentinel(true, "InferFiltersFromGenerate.apply.else46")
//        generate
//      }
//  }
//  private def canInferFilters(g: Generator): Boolean = g match {
//    case _: ExplodeBase if sentinel(true, "InferFiltersFromGenerate.canInferFilters.match1") =>
//      Rule.coverage += "InferFiltersFromGenerate.canInferFilters.match1"
//      true
//    case _: Inline if sentinel(true, "InferFiltersFromGenerate.canInferFilters.match2") =>
//      Rule.coverage += "InferFiltersFromGenerate.canInferFilters.match2"
//      true
//    case _ if sentinel(true, "InferFiltersFromGenerate.canInferFilters.match3") =>
//      Rule.coverage += "InferFiltersFromGenerate.canInferFilters.match3"
//      false
//  }
//}
//object InferFiltersFromConstraints extends Rule[LogicalPlan] with PredicateHelper with ConstraintHelper {
//  def apply(plan: LogicalPlan): LogicalPlan = {
//    if (sentinel(true, "InferFiltersFromConstraints.apply.then1") && conf.constraintPropagationEnabled) {
//      Rule.coverage += "InferFiltersFromConstraints.apply.then1"
//      inferFilters(plan)
//    } else {
//      Rule.coverage += "InferFiltersFromConstraints.apply.else2"
//      sentinel(true, "InferFiltersFromConstraints.apply.else2")
//      plan
//    }
//  }
//  private def inferFilters(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsAnyPattern(FILTER, JOIN)) {
//    case filter @ Filter(condition, child) if sentinel(true, "InferFiltersFromConstraints.inferFilters.case1") =>
//      Rule.coverage += "InferFiltersFromConstraints.inferFilters.case1"
//      val newFilters = filter.constraints -- (child.constraints ++ splitConjunctivePredicates(condition))
//      if (sentinel(true, "InferFiltersFromConstraints.inferFilters.then49") && newFilters.nonEmpty) {
//        Rule.coverage += "InferFiltersFromConstraints.inferFilters.then49"
//        Filter(And(newFilters.reduce(And), condition), child)
//      } else {
//        Rule.coverage += "InferFiltersFromConstraints.inferFilters.else50"
//        sentinel(true, "InferFiltersFromConstraints.inferFilters.else50")
//        filter
//      }
//    case join @ Join(left, right, joinType, conditionOpt, _) if sentinel(true, "InferFiltersFromConstraints.inferFilters.case2") =>
//      Rule.coverage += "InferFiltersFromConstraints.inferFilters.case2"
//      joinType match {
//        case _: InnerLike | LeftSemi if sentinel(true, "InferFiltersFromConstraints.inferFilters.match51") =>
//          Rule.coverage += "InferFiltersFromConstraints.inferFilters.match51"
//          val allConstraints = getAllConstraints(left, right, conditionOpt)
//          val newLeft = inferNewFilter(left, allConstraints)
//          val newRight = inferNewFilter(right, allConstraints)
//          join.copy(left = newLeft, right = newRight)
//        case RightOuter if sentinel(true, "InferFiltersFromConstraints.inferFilters.match52") =>
//          Rule.coverage += "InferFiltersFromConstraints.inferFilters.match52"
//          val allConstraints = getAllConstraints(left, right, conditionOpt)
//          val newLeft = inferNewFilter(left, allConstraints)
//          join.copy(left = newLeft)
//        case LeftOuter | LeftAnti if sentinel(true, "InferFiltersFromConstraints.inferFilters.match53") =>
//          Rule.coverage += "InferFiltersFromConstraints.inferFilters.match53"
//          val allConstraints = getAllConstraints(left, right, conditionOpt)
//          val newRight = inferNewFilter(right, allConstraints)
//          join.copy(right = newRight)
//        case _ if sentinel(true, "InferFiltersFromConstraints.inferFilters.match54") =>
//          Rule.coverage += "InferFiltersFromConstraints.inferFilters.match54"
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
//    if (sentinel(true, "InferFiltersFromConstraints.inferNewFilter.then1") && newPredicates.isEmpty) {
//      Rule.coverage += "InferFiltersFromConstraints.inferNewFilter.then1"
//      plan
//    } else {
//      Rule.coverage += "InferFiltersFromConstraints.inferNewFilter.else2"
//      sentinel(true, "InferFiltersFromConstraints.inferNewFilter.else2")
//      Filter(newPredicates.reduce(And), plan)
//    }
//  }
//}
//object CombineUnions extends Rule[LogicalPlan] {
//  import CollapseProject.{ buildCleanedProjectList, canCollapseExpressions }
//  import PushProjectionThroughUnion.pushProjectionThroughUnion
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformDownWithPruning(_.containsAnyPattern(UNION, DISTINCT_LIKE), ruleId) {
//    case u: Union if sentinel(true, "CombineUnions.apply.case1") =>
//      Rule.coverage += "CombineUnions.apply.case1"
//      flattenUnion(u, false)
//    case Distinct(u: Union) if sentinel(true, "CombineUnions.apply.case2") =>
//      Rule.coverage += "CombineUnions.apply.case2"
//      Distinct(flattenUnion(u, true))
//    case Deduplicate(keys: Seq[Attribute], u: Union) if sentinel(true, "CombineUnions.apply.case3") && AttributeSet(keys) == u.outputSet =>
//      Rule.coverage += "CombineUnions.apply.case3"
//      Deduplicate(keys, flattenUnion(u, true))
//    case DeduplicateWithinWatermark(keys: Seq[Attribute], u: Union) if sentinel(true, "CombineUnions.apply.case4") && AttributeSet(keys) == u.outputSet =>
//      Rule.coverage += "CombineUnions.apply.case4"
//      DeduplicateWithinWatermark(keys, flattenUnion(u, true))
//  }
//  private def flattenUnion(union: Union, flattenDistinct: Boolean): Union = {
//    val topByName = union.byName
//    val topAllowMissingCol = union.allowMissingCol
//    val stack = mutable.Stack[LogicalPlan](union)
//    val flattened = mutable.ArrayBuffer.empty[LogicalPlan]
//    while (stack.nonEmpty) {
//      stack.pop() match {
//        case p1 @ Project(_, p2: Project) if sentinel(true, "CombineUnions.flattenUnion.match1") && (canCollapseExpressions(p1.projectList, p2.projectList, alwaysInline = false) && !p1.projectList.exists(SubqueryExpression.hasCorrelatedSubquery) && !p2.projectList.exists(SubqueryExpression.hasCorrelatedSubquery)) =>
//          Rule.coverage += "CombineUnions.flattenUnion.match1"
//          val newProjectList = buildCleanedProjectList(p1.projectList, p2.projectList)
//          stack.pushAll(Seq(p2.copy(projectList = newProjectList)))
//        case Distinct(Union(children, byName, allowMissingCol)) if sentinel(true, "CombineUnions.flattenUnion.match2") && (flattenDistinct && byName == topByName && allowMissingCol == topAllowMissingCol) =>
//          Rule.coverage += "CombineUnions.flattenUnion.match2"
//          stack.pushAll(children.reverse)
//        case Deduplicate(keys: Seq[Attribute], u: Union) if sentinel(true, "CombineUnions.flattenUnion.match3") && (flattenDistinct && u.byName == topByName && u.allowMissingCol == topAllowMissingCol && AttributeSet(keys) == u.outputSet) =>
//          Rule.coverage += "CombineUnions.flattenUnion.match3"
//          stack.pushAll(u.children.reverse)
//        case Union(children, byName, allowMissingCol) if sentinel(true, "CombineUnions.flattenUnion.match4") && (byName == topByName && allowMissingCol == topAllowMissingCol) =>
//          Rule.coverage += "CombineUnions.flattenUnion.match4"
//          stack.pushAll(children.reverse)
//        case Project(projectList, Distinct(u @ Union(children, byName, allowMissingCol))) if sentinel(true, "CombineUnions.flattenUnion.match5") && (projectList.forall(_.deterministic) && children.nonEmpty && flattenDistinct && byName == topByName && allowMissingCol == topAllowMissingCol) =>
//          Rule.coverage += "CombineUnions.flattenUnion.match5"
//          stack.pushAll(pushProjectionThroughUnion(projectList, u).reverse)
//        case Project(projectList, Deduplicate(keys: Seq[Attribute], u: Union)) if sentinel(true, "CombineUnions.flattenUnion.match6") && (projectList.forall(_.deterministic) && flattenDistinct && u.byName == topByName && u.allowMissingCol == topAllowMissingCol && AttributeSet(keys) == u.outputSet) =>
//          Rule.coverage += "CombineUnions.flattenUnion.match6"
//          stack.pushAll(pushProjectionThroughUnion(projectList, u).reverse)
//        case Project(projectList, u @ Union(children, byName, allowMissingCol)) if sentinel(true, "CombineUnions.flattenUnion.match7") && (projectList.forall(_.deterministic) && children.nonEmpty && byName == topByName && allowMissingCol == topAllowMissingCol) =>
//          Rule.coverage += "CombineUnions.flattenUnion.match7"
//          stack.pushAll(pushProjectionThroughUnion(projectList, u).reverse)
//        case child if sentinel(true, "CombineUnions.flattenUnion.match8") =>
//          Rule.coverage += "CombineUnions.flattenUnion.match8"
//          flattened += child
//      }
//    }
//    union.copy(children = flattened.toSeq)
//  }
//}
//object CombineFilters extends Rule[LogicalPlan] with PredicateHelper {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(FILTER), ruleId)(applyLocally)
//  val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
//    case Filter(fc, nf @ Filter(nc, grandChild)) if sentinel(true, "CombineFilters.apply.case1") && nc.deterministic =>
//      Rule.coverage += "CombineFilters.apply.case1"
//      val (combineCandidates, nonDeterministic) = splitConjunctivePredicates(fc).partition(_.deterministic)
//      val mergedFilter = (ExpressionSet(combineCandidates) -- ExpressionSet(splitConjunctivePredicates(nc))).reduceOption(And) match {
//        case Some(ac) if sentinel(true, "CombineFilters.apply.match55") =>
//          Rule.coverage += "CombineFilters.apply.match55"
//          Filter(And(nc, ac), grandChild)
//        case None if sentinel(true, "CombineFilters.apply.match56") =>
//          Rule.coverage += "CombineFilters.apply.match56"
//          nf
//      }
//      nonDeterministic.reduceOption(And).map(c => Filter(c, mergedFilter)).getOrElse(mergedFilter)
//  }
//}
//object EliminateSorts extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(SORT))(applyLocally)
//  private val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
//    case s @ Sort(orders, _, child) if sentinel(true, "EliminateSorts.apply.case1") && (orders.isEmpty || orders.exists(_.child.foldable)) =>
//      Rule.coverage += "EliminateSorts.apply.case1"
//      val newOrders = orders.filterNot(_.child.foldable)
//      if (sentinel(true, "EliminateSorts.apply.then57") && newOrders.isEmpty) {
//        Rule.coverage += "EliminateSorts.apply.then57"
//        applyLocally.lift(child).getOrElse(child)
//      } else {
//        Rule.coverage += "EliminateSorts.apply.else58"
//        sentinel(true, "EliminateSorts.apply.else58")
//        s.copy(order = newOrders)
//      }
//    case Sort(orders, false, child) if sentinel(true, "EliminateSorts.apply.case2") && SortOrder.orderingSatisfies(child.outputOrdering, orders) =>
//      Rule.coverage += "EliminateSorts.apply.case2"
//      applyLocally.lift(child).getOrElse(child)
//    case s @ Sort(_, global, child) if sentinel(true, "EliminateSorts.apply.case3") =>
//      Rule.coverage += "EliminateSorts.apply.case3"
//      s.copy(child = recursiveRemoveSort(child, global))
//    case j @ Join(originLeft, originRight, _, cond, _) if sentinel(true, "EliminateSorts.apply.case4") && cond.forall(_.deterministic) =>
//      Rule.coverage += "EliminateSorts.apply.case4"
//      j.copy(left = recursiveRemoveSort(originLeft, true), right = recursiveRemoveSort(originRight, true))
//    case g @ Aggregate(_, aggs, originChild) if sentinel(true, "EliminateSorts.apply.case5") && isOrderIrrelevantAggs(aggs) =>
//      Rule.coverage += "EliminateSorts.apply.case5"
//      g.copy(child = recursiveRemoveSort(originChild, true))
//  }
//  private def recursiveRemoveSort(plan: LogicalPlan, canRemoveGlobalSort: Boolean): LogicalPlan = {
//    if (sentinel(true, "EliminateSorts.recursiveRemoveSort.then1") && !plan.containsPattern(SORT)) {
//      Rule.coverage += "EliminateSorts.recursiveRemoveSort.then1"
//      return plan
//    } else {
//      Rule.coverage += "EliminateSorts.recursiveRemoveSort.else2"
//      sentinel(true, "EliminateSorts.recursiveRemoveSort.else2")
//    }
//    plan match {
//      case Sort(_, global, child) if sentinel(true, "EliminateSorts.recursiveRemoveSort.match3") && (canRemoveGlobalSort || !global) =>
//        Rule.coverage += "EliminateSorts.recursiveRemoveSort.match3"
//        recursiveRemoveSort(child, canRemoveGlobalSort)
//      case Sort(sortOrder, true, child) if sentinel(true, "EliminateSorts.recursiveRemoveSort.match4") =>
//        Rule.coverage += "EliminateSorts.recursiveRemoveSort.match4"
//        RepartitionByExpression(sortOrder, recursiveRemoveSort(child, true), None)
//      case other if sentinel(true, "EliminateSorts.recursiveRemoveSort.match5") && canEliminateSort(other) =>
//        Rule.coverage += "EliminateSorts.recursiveRemoveSort.match5"
//        other.withNewChildren(other.children.map(c => recursiveRemoveSort(c, canRemoveGlobalSort)))
//      case other if sentinel(true, "EliminateSorts.recursiveRemoveSort.match6") && canEliminateGlobalSort(other) =>
//        Rule.coverage += "EliminateSorts.recursiveRemoveSort.match6"
//        other.withNewChildren(other.children.map(c => recursiveRemoveSort(c, true)))
//      case _ if sentinel(true, "EliminateSorts.recursiveRemoveSort.match7") =>
//        Rule.coverage += "EliminateSorts.recursiveRemoveSort.match7"
//        plan
//    }
//  }
//  private def canEliminateSort(plan: LogicalPlan): Boolean = plan match {
//    case p: Project if sentinel(true, "EliminateSorts.canEliminateSort.match1") =>
//      Rule.coverage += "EliminateSorts.canEliminateSort.match1"
//      p.projectList.forall(_.deterministic)
//    case f: Filter if sentinel(true, "EliminateSorts.canEliminateSort.match2") =>
//      Rule.coverage += "EliminateSorts.canEliminateSort.match2"
//      f.condition.deterministic
//    case _: LocalLimit if sentinel(true, "EliminateSorts.canEliminateSort.match3") =>
//      Rule.coverage += "EliminateSorts.canEliminateSort.match3"
//      true
//    case _ if sentinel(true, "EliminateSorts.canEliminateSort.match4") =>
//      Rule.coverage += "EliminateSorts.canEliminateSort.match4"
//      false
//  }
//  private def canEliminateGlobalSort(plan: LogicalPlan): Boolean = plan match {
//    case r: RepartitionByExpression if sentinel(true, "EliminateSorts.canEliminateGlobalSort.match1") =>
//      Rule.coverage += "EliminateSorts.canEliminateGlobalSort.match1"
//      r.partitionExpressions.forall(_.deterministic)
//    case r: RebalancePartitions if sentinel(true, "EliminateSorts.canEliminateGlobalSort.match2") =>
//      Rule.coverage += "EliminateSorts.canEliminateGlobalSort.match2"
//      r.partitionExpressions.forall(_.deterministic)
//    case _: Repartition if sentinel(true, "EliminateSorts.canEliminateGlobalSort.match3") =>
//      Rule.coverage += "EliminateSorts.canEliminateGlobalSort.match3"
//      true
//    case _ if sentinel(true, "EliminateSorts.canEliminateGlobalSort.match4") =>
//      Rule.coverage += "EliminateSorts.canEliminateGlobalSort.match4"
//      false
//  }
//  private def isOrderIrrelevantAggs(aggs: Seq[NamedExpression]): Boolean = {
//    def isOrderIrrelevantAggFunction(func: AggregateFunction): Boolean = func match {
//      case _: Min | _: Max | _: Count | _: BitAggregate if sentinel(true, "EliminateSorts.isOrderIrrelevantAggFunction.match1") =>
//        Rule.coverage += "EliminateSorts.isOrderIrrelevantAggFunction.match1"
//        true
//      case _: Sum | _: Average | _: CentralMomentAgg if sentinel(true, "EliminateSorts.isOrderIrrelevantAggFunction.match2") =>
//        Rule.coverage += "EliminateSorts.isOrderIrrelevantAggFunction.match2"
//        !Seq(FloatType, DoubleType).exists(e => DataTypeUtils.sameType(e, func.children.head.dataType))
//      case _ if sentinel(true, "EliminateSorts.isOrderIrrelevantAggFunction.match3") =>
//        Rule.coverage += "EliminateSorts.isOrderIrrelevantAggFunction.match3"
//        false
//    }
//    def checkValidAggregateExpression(expr: Expression): Boolean = expr match {
//      case _: AttributeReference if sentinel(true, "EliminateSorts.checkValidAggregateExpression.match1") =>
//        Rule.coverage += "EliminateSorts.checkValidAggregateExpression.match1"
//        true
//      case ae: AggregateExpression if sentinel(true, "EliminateSorts.checkValidAggregateExpression.match2") =>
//        Rule.coverage += "EliminateSorts.checkValidAggregateExpression.match2"
//        isOrderIrrelevantAggFunction(ae.aggregateFunction)
//      case _: UserDefinedExpression if sentinel(true, "EliminateSorts.checkValidAggregateExpression.match3") =>
//        Rule.coverage += "EliminateSorts.checkValidAggregateExpression.match3"
//        false
//      case e if sentinel(true, "EliminateSorts.checkValidAggregateExpression.match4") =>
//        Rule.coverage += "EliminateSorts.checkValidAggregateExpression.match4"
//        e.children.forall(checkValidAggregateExpression)
//    }
//    aggs.forall(checkValidAggregateExpression)
//  }
//}
//object PruneFilters extends Rule[LogicalPlan] with PredicateHelper {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(FILTER), ruleId) {
//    case Filter(Literal(true, BooleanType), child) if sentinel(true, "PruneFilters.apply.case1") =>
//      Rule.coverage += "PruneFilters.apply.case1"
//      child
//    case Filter(Literal(null, _), child) if sentinel(true, "PruneFilters.apply.case2") =>
//      Rule.coverage += "PruneFilters.apply.case2"
//      LocalRelation(child.output, data = Seq.empty, isStreaming = plan.isStreaming)
//    case Filter(Literal(false, BooleanType), child) if sentinel(true, "PruneFilters.apply.case3") =>
//      Rule.coverage += "PruneFilters.apply.case3"
//      LocalRelation(child.output, data = Seq.empty, isStreaming = plan.isStreaming)
//    case f @ Filter(fc, p: LogicalPlan) if sentinel(true, "PruneFilters.apply.case4") =>
//      Rule.coverage += "PruneFilters.apply.case4"
//      val (prunedPredicates, remainingPredicates) = splitConjunctivePredicates(fc).partition {
//        cond => cond.deterministic && p.constraints.contains(cond)
//      }
//      if (sentinel(true, "PruneFilters.apply.then59") && prunedPredicates.isEmpty) {
//        Rule.coverage += "PruneFilters.apply.then59"
//        f
//      } else if (sentinel(true, "PruneFilters.apply.elseif60") && (sentinel(true, "PruneFilters.apply.elseif60") && remainingPredicates.isEmpty)) {
//        Rule.coverage += "PruneFilters.apply.elseif60"
//        p
//      } else {
//        Rule.coverage += "PruneFilters.apply.else61"
//        sentinel(true, "PruneFilters.apply.else61")
//        sentinel(true, "PruneFilters.apply.else61")
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
//    case Filter(condition, project @ Project(fields, grandChild)) if sentinel(true, "PushPredicateThroughNonJoin.apply.case1") && (fields.forall(_.deterministic) && canPushThroughCondition(grandChild, condition)) =>
//      Rule.coverage += "PushPredicateThroughNonJoin.apply.case1"
//      val aliasMap = getAliasMap(project)
//      project.copy(child = Filter(replaceAlias(condition, aliasMap), grandChild))
//    case filter @ Filter(condition, aggregate: Aggregate) if sentinel(true, "PushPredicateThroughNonJoin.apply.case2") && (aggregate.aggregateExpressions.forall(_.deterministic) && aggregate.groupingExpressions.nonEmpty) =>
//      Rule.coverage += "PushPredicateThroughNonJoin.apply.case2"
//      val aliasMap = getAliasMap(aggregate)
//      val (candidates, nonDeterministic) = splitConjunctivePredicates(condition).partition(_.deterministic)
//      val (pushDown, rest) = candidates.partition { cond =>
//        val replaced = replaceAlias(cond, aliasMap)
//        cond.references.nonEmpty && replaced.references.subsetOf(aggregate.child.outputSet)
//      }
//      val stayUp = rest ++ nonDeterministic
//      if (sentinel(true, "PushPredicateThroughNonJoin.apply.then64") && pushDown.nonEmpty) {
//        Rule.coverage += "PushPredicateThroughNonJoin.apply.then64"
//        val pushDownPredicate = pushDown.reduce(And)
//        val replaced = replaceAlias(pushDownPredicate, aliasMap)
//        val newAggregate = aggregate.copy(child = Filter(replaced, aggregate.child))
//        if (sentinel(true, "PushPredicateThroughNonJoin.apply.then66") && stayUp.isEmpty) {
//          Rule.coverage += "PushPredicateThroughNonJoin.apply.then66"
//          newAggregate
//        } else {
//          Rule.coverage += "PushPredicateThroughNonJoin.apply.else67"
//          sentinel(true, "PushPredicateThroughNonJoin.apply.else67")
//          Filter(stayUp.reduce(And), newAggregate)
//        }
//      } else {
//        Rule.coverage += "PushPredicateThroughNonJoin.apply.else65"
//        sentinel(true, "PushPredicateThroughNonJoin.apply.else65")
//        filter
//      }
//    case filter @ Filter(condition, w: Window) if sentinel(true, "PushPredicateThroughNonJoin.apply.case3") && w.partitionSpec.forall(_.isInstanceOf[AttributeReference]) =>
//      Rule.coverage += "PushPredicateThroughNonJoin.apply.case3"
//      val partitionAttrs = AttributeSet(w.partitionSpec.flatMap(_.references))
//      val (candidates, nonDeterministic) = splitConjunctivePredicates(condition).partition(_.deterministic)
//      val (pushDown, rest) = candidates.partition {
//        cond => cond.references.subsetOf(partitionAttrs)
//      }
//      val stayUp = rest ++ nonDeterministic
//      if (sentinel(true, "PushPredicateThroughNonJoin.apply.then68") && pushDown.nonEmpty) {
//        Rule.coverage += "PushPredicateThroughNonJoin.apply.then68"
//        val pushDownPredicate = pushDown.reduce(And)
//        val newWindow = w.copy(child = Filter(pushDownPredicate, w.child))
//        if (sentinel(true, "PushPredicateThroughNonJoin.apply.then70") && stayUp.isEmpty) {
//          Rule.coverage += "PushPredicateThroughNonJoin.apply.then70"
//          newWindow
//        } else {
//          Rule.coverage += "PushPredicateThroughNonJoin.apply.else71"
//          sentinel(true, "PushPredicateThroughNonJoin.apply.else71")
//          Filter(stayUp.reduce(And), newWindow)
//        }
//      } else {
//        Rule.coverage += "PushPredicateThroughNonJoin.apply.else69"
//        sentinel(true, "PushPredicateThroughNonJoin.apply.else69")
//        filter
//      }
//    case filter @ Filter(condition, union: Union) if sentinel(true, "PushPredicateThroughNonJoin.apply.case4") =>
//      Rule.coverage += "PushPredicateThroughNonJoin.apply.case4"
//      val (pushDown, stayUp) = splitConjunctivePredicates(condition).partition(_.deterministic)
//      if (sentinel(true, "PushPredicateThroughNonJoin.apply.then72") && pushDown.nonEmpty) {
//        Rule.coverage += "PushPredicateThroughNonJoin.apply.then72"
//        val pushDownCond = pushDown.reduceLeft(And)
//        val output = union.output
//        val newGrandChildren = union.children.map { grandchild =>
//          val newCond = pushDownCond transform {
//            case e if sentinel(true, "PushPredicateThroughNonJoin.apply.case74") && output.exists(_.semanticEquals(e)) =>
//              Rule.coverage += "PushPredicateThroughNonJoin.apply.case74"
//              grandchild.output(output.indexWhere(_.semanticEquals(e)))
//          }
//          assert(newCond.references.subsetOf(grandchild.outputSet))
//          Filter(newCond, grandchild)
//        }
//        val newUnion = union.withNewChildren(newGrandChildren)
//        if (sentinel(true, "PushPredicateThroughNonJoin.apply.then75") && stayUp.nonEmpty) {
//          Rule.coverage += "PushPredicateThroughNonJoin.apply.then75"
//          Filter(stayUp.reduceLeft(And), newUnion)
//        } else {
//          Rule.coverage += "PushPredicateThroughNonJoin.apply.else76"
//          sentinel(true, "PushPredicateThroughNonJoin.apply.else76")
//          newUnion
//        }
//      } else {
//        Rule.coverage += "PushPredicateThroughNonJoin.apply.else73"
//        sentinel(true, "PushPredicateThroughNonJoin.apply.else73")
//        filter
//      }
//    case filter @ Filter(condition, watermark: EventTimeWatermark) if sentinel(true, "PushPredicateThroughNonJoin.apply.case5") =>
//      Rule.coverage += "PushPredicateThroughNonJoin.apply.case5"
//      val (pushDown, stayUp) = splitConjunctivePredicates(condition).partition {
//        p => p.deterministic && !p.references.contains(watermark.eventTime)
//      }
//      if (sentinel(true, "PushPredicateThroughNonJoin.apply.then77") && pushDown.nonEmpty) {
//        Rule.coverage += "PushPredicateThroughNonJoin.apply.then77"
//        val pushDownPredicate = pushDown.reduceLeft(And)
//        val newWatermark = watermark.copy(child = Filter(pushDownPredicate, watermark.child))
//        if (sentinel(true, "PushPredicateThroughNonJoin.apply.then79") && stayUp.isEmpty) {
//          Rule.coverage += "PushPredicateThroughNonJoin.apply.then79"
//          newWatermark
//        } else {
//          Rule.coverage += "PushPredicateThroughNonJoin.apply.else80"
//          sentinel(true, "PushPredicateThroughNonJoin.apply.else80")
//          Filter(stayUp.reduceLeft(And), newWatermark)
//        }
//      } else {
//        Rule.coverage += "PushPredicateThroughNonJoin.apply.else78"
//        sentinel(true, "PushPredicateThroughNonJoin.apply.else78")
//        filter
//      }
//    case filter @ Filter(_, u: UnaryNode) if sentinel(true, "PushPredicateThroughNonJoin.apply.case6") && (canPushThrough(u) && u.expressions.forall(_.deterministic)) =>
//      Rule.coverage += "PushPredicateThroughNonJoin.apply.case6"
//      pushDownPredicate(filter, u.child) {
//        predicate => u.withNewChildren(Seq(Filter(predicate, u.child)))
//      }
//  }
//  def canPushThrough(p: UnaryNode): Boolean = p match {
//    case _: AppendColumns if sentinel(true, "PushPredicateThroughNonJoin.canPushThrough.match1") =>
//      Rule.coverage += "PushPredicateThroughNonJoin.canPushThrough.match1"
//      true
//    case _: Distinct if sentinel(true, "PushPredicateThroughNonJoin.canPushThrough.match2") =>
//      Rule.coverage += "PushPredicateThroughNonJoin.canPushThrough.match2"
//      true
//    case _: Generate if sentinel(true, "PushPredicateThroughNonJoin.canPushThrough.match3") =>
//      Rule.coverage += "PushPredicateThroughNonJoin.canPushThrough.match3"
//      true
//    case _: Pivot if sentinel(true, "PushPredicateThroughNonJoin.canPushThrough.match4") =>
//      Rule.coverage += "PushPredicateThroughNonJoin.canPushThrough.match4"
//      true
//    case _: RepartitionByExpression if sentinel(true, "PushPredicateThroughNonJoin.canPushThrough.match5") =>
//      Rule.coverage += "PushPredicateThroughNonJoin.canPushThrough.match5"
//      true
//    case _: Repartition if sentinel(true, "PushPredicateThroughNonJoin.canPushThrough.match6") =>
//      Rule.coverage += "PushPredicateThroughNonJoin.canPushThrough.match6"
//      true
//    case _: RebalancePartitions if sentinel(true, "PushPredicateThroughNonJoin.canPushThrough.match7") =>
//      Rule.coverage += "PushPredicateThroughNonJoin.canPushThrough.match7"
//      true
//    case _: ScriptTransformation if sentinel(true, "PushPredicateThroughNonJoin.canPushThrough.match8") =>
//      Rule.coverage += "PushPredicateThroughNonJoin.canPushThrough.match8"
//      true
//    case _: Sort if sentinel(true, "PushPredicateThroughNonJoin.canPushThrough.match9") =>
//      Rule.coverage += "PushPredicateThroughNonJoin.canPushThrough.match9"
//      true
//    case _: BatchEvalPython if sentinel(true, "PushPredicateThroughNonJoin.canPushThrough.match10") =>
//      Rule.coverage += "PushPredicateThroughNonJoin.canPushThrough.match10"
//      true
//    case _: ArrowEvalPython if sentinel(true, "PushPredicateThroughNonJoin.canPushThrough.match11") =>
//      Rule.coverage += "PushPredicateThroughNonJoin.canPushThrough.match11"
//      true
//    case _: Expand if sentinel(true, "PushPredicateThroughNonJoin.canPushThrough.match12") =>
//      Rule.coverage += "PushPredicateThroughNonJoin.canPushThrough.match12"
//      true
//    case _ if sentinel(true, "PushPredicateThroughNonJoin.canPushThrough.match13") =>
//      Rule.coverage += "PushPredicateThroughNonJoin.canPushThrough.match13"
//      false
//  }
//  private def pushDownPredicate(filter: Filter, grandchild: LogicalPlan)(insertFilter: Expression => LogicalPlan): LogicalPlan = {
//    val (candidates, nonDeterministic) = splitConjunctivePredicates(filter.condition).partition(_.deterministic)
//    val (pushDown, rest) = candidates.partition {
//      cond => cond.references.subsetOf(grandchild.outputSet)
//    }
//    val stayUp = rest ++ nonDeterministic
//    if (sentinel(true, "PushPredicateThroughNonJoin.pushDownPredicate.then1") && pushDown.nonEmpty) {
//      Rule.coverage += "PushPredicateThroughNonJoin.pushDownPredicate.then1"
//      val newChild = insertFilter(pushDown.reduceLeft(And))
//      if (sentinel(true, "PushPredicateThroughNonJoin.pushDownPredicate.then81") && stayUp.nonEmpty) {
//        Rule.coverage += "PushPredicateThroughNonJoin.pushDownPredicate.then81"
//        Filter(stayUp.reduceLeft(And), newChild)
//      } else {
//        Rule.coverage += "PushPredicateThroughNonJoin.pushDownPredicate.else82"
//        sentinel(true, "PushPredicateThroughNonJoin.pushDownPredicate.else82")
//        newChild
//      }
//    } else {
//      Rule.coverage += "PushPredicateThroughNonJoin.pushDownPredicate.else2"
//      sentinel(true, "PushPredicateThroughNonJoin.pushDownPredicate.else2")
//      filter
//    }
//  }
//  private def canPushThroughCondition(plan: LogicalPlan, condition: Expression): Boolean = {
//    val attributes = plan.outputSet
//    !condition.exists {
//      case s: SubqueryExpression if sentinel(true, "PushPredicateThroughNonJoin.canPushThroughCondition.case1") =>
//        Rule.coverage += "PushPredicateThroughNonJoin.canPushThroughCondition.case1"
//        s.plan.outputSet.intersect(attributes).nonEmpty
//      case _ if sentinel(true, "PushPredicateThroughNonJoin.canPushThroughCondition.case2") =>
//        Rule.coverage += "PushPredicateThroughNonJoin.canPushThroughCondition.case2"
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
//    case _: InnerLike | LeftSemi | RightOuter | LeftOuter | LeftAnti | ExistenceJoin(_) if sentinel(true, "PushPredicateThroughJoin.canPushThrough.match1") =>
//      Rule.coverage += "PushPredicateThroughJoin.canPushThrough.match1"
//      true
//    case _ if sentinel(true, "PushPredicateThroughJoin.canPushThrough.match2") =>
//      Rule.coverage += "PushPredicateThroughJoin.canPushThrough.match2"
//      false
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform applyLocally
//  val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
//    case f @ Filter(filterCondition, Join(left, right, joinType, joinCondition, hint)) if sentinel(true, "PushPredicateThroughJoin.apply.case1") && canPushThrough(joinType) =>
//      Rule.coverage += "PushPredicateThroughJoin.apply.case1"
//      val (leftFilterConditions, rightFilterConditions, commonFilterCondition) = split(splitConjunctivePredicates(filterCondition), left, right)
//      joinType match {
//        case _: InnerLike if sentinel(true, "PushPredicateThroughJoin.apply.match83") =>
//          Rule.coverage += "PushPredicateThroughJoin.apply.match83"
//          val newLeft = leftFilterConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
//          val newRight = rightFilterConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
//          val (newJoinConditions, others) = commonFilterCondition.partition(canEvaluateWithinJoin)
//          val newJoinCond = (newJoinConditions ++ joinCondition).reduceLeftOption(And)
//          val join = Join(newLeft, newRight, joinType, newJoinCond, hint)
//          if (sentinel(true, "PushPredicateThroughJoin.apply.then87") && others.nonEmpty) {
//            Rule.coverage += "PushPredicateThroughJoin.apply.then87"
//            Filter(others.reduceLeft(And), join)
//          } else {
//            Rule.coverage += "PushPredicateThroughJoin.apply.else88"
//            sentinel(true, "PushPredicateThroughJoin.apply.else88")
//            join
//          }
//        case RightOuter if sentinel(true, "PushPredicateThroughJoin.apply.match84") =>
//          Rule.coverage += "PushPredicateThroughJoin.apply.match84"
//          val newLeft = left
//          val newRight = rightFilterConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
//          val newJoinCond = joinCondition
//          val newJoin = Join(newLeft, newRight, RightOuter, newJoinCond, hint)
//          (leftFilterConditions ++ commonFilterCondition).reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
//        case LeftOuter | LeftExistence(_) if sentinel(true, "PushPredicateThroughJoin.apply.match85") =>
//          Rule.coverage += "PushPredicateThroughJoin.apply.match85"
//          val newLeft = leftFilterConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
//          val newRight = right
//          val newJoinCond = joinCondition
//          val newJoin = Join(newLeft, newRight, joinType, newJoinCond, hint)
//          (rightFilterConditions ++ commonFilterCondition).reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
//        case other if sentinel(true, "PushPredicateThroughJoin.apply.match86") =>
//          Rule.coverage += "PushPredicateThroughJoin.apply.match86"
//          throw new IllegalStateException(s"Unexpected join type: $other")
//      }
//    case j @ Join(left, right, joinType, joinCondition, hint) if sentinel(true, "PushPredicateThroughJoin.apply.case2") && canPushThrough(joinType) =>
//      Rule.coverage += "PushPredicateThroughJoin.apply.case2"
//      val (leftJoinConditions, rightJoinConditions, commonJoinCondition) = split(joinCondition.map(splitConjunctivePredicates).getOrElse(Nil), left, right)
//      joinType match {
//        case _: InnerLike | LeftSemi if sentinel(true, "PushPredicateThroughJoin.apply.match89") =>
//          Rule.coverage += "PushPredicateThroughJoin.apply.match89"
//          val newLeft = leftJoinConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
//          val newRight = rightJoinConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
//          val newJoinCond = commonJoinCondition.reduceLeftOption(And)
//          Join(newLeft, newRight, joinType, newJoinCond, hint)
//        case RightOuter if sentinel(true, "PushPredicateThroughJoin.apply.match90") =>
//          Rule.coverage += "PushPredicateThroughJoin.apply.match90"
//          val newLeft = leftJoinConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
//          val newRight = right
//          val newJoinCond = (rightJoinConditions ++ commonJoinCondition).reduceLeftOption(And)
//          Join(newLeft, newRight, RightOuter, newJoinCond, hint)
//        case LeftOuter | LeftAnti | ExistenceJoin(_) if sentinel(true, "PushPredicateThroughJoin.apply.match91") =>
//          Rule.coverage += "PushPredicateThroughJoin.apply.match91"
//          val newLeft = left
//          val newRight = rightJoinConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
//          val newJoinCond = (leftJoinConditions ++ commonJoinCondition).reduceLeftOption(And)
//          Join(newLeft, newRight, joinType, newJoinCond, hint)
//        case other if sentinel(true, "PushPredicateThroughJoin.apply.match92") =>
//          Rule.coverage += "PushPredicateThroughJoin.apply.match92"
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
//    case Limit(l, child) if sentinel(true, "EliminateLimits.apply.case1") && canEliminate(l, child) =>
//      Rule.coverage += "EliminateLimits.apply.case1"
//      child
//    case GlobalLimit(l, child) if sentinel(true, "EliminateLimits.apply.case2") && canEliminate(l, child) =>
//      Rule.coverage += "EliminateLimits.apply.case2"
//      child
//    case LocalLimit(IntegerLiteral(0), child) if sentinel(true, "EliminateLimits.apply.case3") =>
//      Rule.coverage += "EliminateLimits.apply.case3"
//      LocalRelation(child.output, data = Seq.empty, isStreaming = child.isStreaming)
//    case GlobalLimit(IntegerLiteral(0), child) if sentinel(true, "EliminateLimits.apply.case4") =>
//      Rule.coverage += "EliminateLimits.apply.case4"
//      LocalRelation(child.output, data = Seq.empty, isStreaming = child.isStreaming)
//    case GlobalLimit(le, GlobalLimit(ne, grandChild)) if sentinel(true, "EliminateLimits.apply.case5") =>
//      Rule.coverage += "EliminateLimits.apply.case5"
//      GlobalLimit(Literal(Least(Seq(ne, le)).eval().asInstanceOf[Int]), grandChild)
//    case LocalLimit(le, LocalLimit(ne, grandChild)) if sentinel(true, "EliminateLimits.apply.case6") =>
//      Rule.coverage += "EliminateLimits.apply.case6"
//      LocalLimit(Literal(Least(Seq(ne, le)).eval().asInstanceOf[Int]), grandChild)
//    case Limit(le, Limit(ne, grandChild)) if sentinel(true, "EliminateLimits.apply.case7") =>
//      Rule.coverage += "EliminateLimits.apply.case7"
//      Limit(Literal(Least(Seq(ne, le)).eval().asInstanceOf[Int]), grandChild)
//  }
//}
//object EliminateOffsets extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case Offset(oe, child) if sentinel(true, "EliminateOffsets.apply.case1") && (oe.foldable && oe.eval().asInstanceOf[Int] == 0) =>
//      Rule.coverage += "EliminateOffsets.apply.case1"
//      child
//    case Offset(oe, child) if sentinel(true, "EliminateOffsets.apply.case2") && (oe.foldable && child.maxRows.exists(_ <= (oe.eval().asInstanceOf[Int]))) =>
//      Rule.coverage += "EliminateOffsets.apply.case2"
//      LocalRelation(child.output, data = Seq.empty, isStreaming = child.isStreaming)
//    case Offset(oe1, Offset(oe2, child)) if sentinel(true, "EliminateOffsets.apply.case3") =>
//      Rule.coverage += "EliminateOffsets.apply.case3"
//      Offset(Add(oe1, oe2), child)
//  }
//}
//object CheckCartesianProducts extends Rule[LogicalPlan] with PredicateHelper {
//  def isCartesianProduct(join: Join): Boolean = {
//    val conditions = join.condition.map(splitConjunctivePredicates).getOrElse(Nil)
//    conditions match {
//      case Seq(Literal.FalseLiteral) | Seq(Literal(null, BooleanType)) if sentinel(true, "CheckCartesianProducts.isCartesianProduct.match1") =>
//        Rule.coverage += "CheckCartesianProducts.isCartesianProduct.match1"
//        false
//      case _ if sentinel(true, "CheckCartesianProducts.isCartesianProduct.match2") =>
//        Rule.coverage += "CheckCartesianProducts.isCartesianProduct.match2"
//        !conditions.map(_.references).exists(refs => refs.exists(join.left.outputSet.contains) && refs.exists(join.right.outputSet.contains))
//    }
//  }
//  def apply(plan: LogicalPlan): LogicalPlan = if (sentinel(true, "CheckCartesianProducts.apply.then1") && conf.crossJoinEnabled) {
//    Rule.coverage += "CheckCartesianProducts.apply.then1"
//    plan
//  } else {
//    Rule.coverage += "CheckCartesianProducts.apply.else2"
//    sentinel(true, "CheckCartesianProducts.apply.else2")
//    plan.transformWithPruning(_.containsAnyPattern(INNER_LIKE_JOIN, OUTER_JOIN)) {
//      case j @ Join(left, right, Inner | LeftOuter | RightOuter | FullOuter, _, _) if sentinel(true, "CheckCartesianProducts.apply.case93") && isCartesianProduct(j) =>
//        Rule.coverage += "CheckCartesianProducts.apply.case93"
//        throw QueryCompilationErrors.joinConditionMissingOrTrivialError(j, left, right)
//    }
//  }
//}
//object DecimalAggregates extends Rule[LogicalPlan] {
//  import Decimal.MAX_LONG_DIGITS
//  private val MAX_DOUBLE_DIGITS = 15
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsAnyPattern(SUM, AVERAGE), ruleId) {
//    case q: LogicalPlan if sentinel(true, "DecimalAggregates.apply.case1") =>
//      Rule.coverage += "DecimalAggregates.apply.case1"
//      q.transformExpressionsDownWithPruning(_.containsAnyPattern(SUM, AVERAGE), ruleId) {
//        case we @ WindowExpression(ae @ AggregateExpression(af, _, _, _, _), _) if sentinel(true, "DecimalAggregates.apply.case94") =>
//          Rule.coverage += "DecimalAggregates.apply.case94"
//          af match {
//            case Sum(e @ DecimalExpression(prec, scale), _) if sentinel(true, "DecimalAggregates.apply.match96") && prec + 10 <= MAX_LONG_DIGITS =>
//              Rule.coverage += "DecimalAggregates.apply.match96"
//              MakeDecimal(we.copy(windowFunction = ae.copy(aggregateFunction = Sum(UnscaledValue(e)))), prec + 10, scale)
//            case Average(e @ DecimalExpression(prec, scale), _) if sentinel(true, "DecimalAggregates.apply.match97") && prec + 4 <= MAX_DOUBLE_DIGITS =>
//              Rule.coverage += "DecimalAggregates.apply.match97"
//              val newAggExpr = we.copy(windowFunction = ae.copy(aggregateFunction = Average(UnscaledValue(e))))
//              Cast(Divide(newAggExpr, Literal.create(math.pow(10.0d, scale), DoubleType)), DecimalType(prec + 4, scale + 4), Option(conf.sessionLocalTimeZone))
//            case _ if sentinel(true, "DecimalAggregates.apply.match98") =>
//              Rule.coverage += "DecimalAggregates.apply.match98"
//              we
//          }
//        case ae @ AggregateExpression(af, _, _, _, _) if sentinel(true, "DecimalAggregates.apply.case95") =>
//          Rule.coverage += "DecimalAggregates.apply.case95"
//          af match {
//            case Sum(e @ DecimalExpression(prec, scale), _) if sentinel(true, "DecimalAggregates.apply.match99") && prec + 10 <= MAX_LONG_DIGITS =>
//              Rule.coverage += "DecimalAggregates.apply.match99"
//              MakeDecimal(ae.copy(aggregateFunction = Sum(UnscaledValue(e))), prec + 10, scale)
//            case Average(e @ DecimalExpression(prec, scale), _) if sentinel(true, "DecimalAggregates.apply.match100") && prec + 4 <= MAX_DOUBLE_DIGITS =>
//              Rule.coverage += "DecimalAggregates.apply.match100"
//              val newAggExpr = ae.copy(aggregateFunction = Average(UnscaledValue(e)))
//              Cast(Divide(newAggExpr, Literal.create(math.pow(10.0d, scale), DoubleType)), DecimalType(prec + 4, scale + 4), Option(conf.sessionLocalTimeZone))
//            case _ if sentinel(true, "DecimalAggregates.apply.match101") =>
//              Rule.coverage += "DecimalAggregates.apply.match101"
//              ae
//          }
//      }
//  }
//}
//object ConvertToLocalRelation extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(LOCAL_RELATION), ruleId) {
//    case Project(projectList, LocalRelation(output, data, isStreaming)) if sentinel(true, "ConvertToLocalRelation.apply.case1") && !projectList.exists(hasUnevaluableExpr) =>
//      Rule.coverage += "ConvertToLocalRelation.apply.case1"
//      val projection = new InterpretedMutableProjection(projectList, output)
//      projection.initialize(0)
//      LocalRelation(projectList.map(_.toAttribute), data.map(projection(_).copy()), isStreaming)
//    case Limit(IntegerLiteral(limit), LocalRelation(output, data, isStreaming)) if sentinel(true, "ConvertToLocalRelation.apply.case2") =>
//      Rule.coverage += "ConvertToLocalRelation.apply.case2"
//      LocalRelation(output, data.take(limit), isStreaming)
//    case Filter(condition, LocalRelation(output, data, isStreaming)) if sentinel(true, "ConvertToLocalRelation.apply.case3") && !hasUnevaluableExpr(condition) =>
//      Rule.coverage += "ConvertToLocalRelation.apply.case3"
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
//    case Distinct(child) if sentinel(true, "ReplaceDistinctWithAggregate.apply.case1") =>
//      Rule.coverage += "ReplaceDistinctWithAggregate.apply.case1"
//      Aggregate(child.output, child.output, child)
//  }
//}
//object ReplaceDeduplicateWithAggregate extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan transformUpWithNewOutput {
//    case d @ Deduplicate(keys, child) if sentinel(true, "ReplaceDeduplicateWithAggregate.apply.case1") && !child.isStreaming =>
//      Rule.coverage += "ReplaceDeduplicateWithAggregate.apply.case1"
//      val keyExprIds = keys.map(_.exprId)
//      val aggCols = child.output.map {
//        attr => if (sentinel(true, "ReplaceDeduplicateWithAggregate.apply.then102") && keyExprIds.contains(attr.exprId)) {
//          Rule.coverage += "ReplaceDeduplicateWithAggregate.apply.then102"
//          attr
//        } else {
//          Rule.coverage += "ReplaceDeduplicateWithAggregate.apply.else103"
//          sentinel(true, "ReplaceDeduplicateWithAggregate.apply.else103")
//          Alias(new First(attr).toAggregateExpression(), attr.name)()
//        }
//      }
//      val nonemptyKeys = if (sentinel(true, "ReplaceDeduplicateWithAggregate.apply.then104") && keys.isEmpty) {
//        Rule.coverage += "ReplaceDeduplicateWithAggregate.apply.then104"
//        Literal(1) :: Nil
//      } else {
//        Rule.coverage += "ReplaceDeduplicateWithAggregate.apply.else105"
//        sentinel(true, "ReplaceDeduplicateWithAggregate.apply.else105")
//        keys
//      }
//      val newAgg = Aggregate(nonemptyKeys, aggCols, child)
//      val attrMapping = d.output.zip(newAgg.output)
//      newAgg -> attrMapping
//  }
//}
//object ReplaceIntersectWithSemiJoin extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(INTERSECT), ruleId) {
//    case Intersect(left, right, false) if sentinel(true, "ReplaceIntersectWithSemiJoin.apply.case1") =>
//      Rule.coverage += "ReplaceIntersectWithSemiJoin.apply.case1"
//      assert(left.output.size == right.output.size)
//      val joinCond = left.output.zip(right.output).map {
//        case (l, r) if sentinel(true, "ReplaceIntersectWithSemiJoin.apply.case106") =>
//          Rule.coverage += "ReplaceIntersectWithSemiJoin.apply.case106"
//          EqualNullSafe(l, r)
//      }
//      Distinct(Join(left, right, LeftSemi, joinCond.reduceLeftOption(And), JoinHint.NONE))
//  }
//}
//object ReplaceExceptWithAntiJoin extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(EXCEPT), ruleId) {
//    case Except(left, right, false) if sentinel(true, "ReplaceExceptWithAntiJoin.apply.case1") =>
//      Rule.coverage += "ReplaceExceptWithAntiJoin.apply.case1"
//      assert(left.output.size == right.output.size)
//      val joinCond = left.output.zip(right.output).map {
//        case (l, r) if sentinel(true, "ReplaceExceptWithAntiJoin.apply.case107") =>
//          Rule.coverage += "ReplaceExceptWithAntiJoin.apply.case107"
//          EqualNullSafe(l, r)
//      }
//      Distinct(Join(left, right, LeftAnti, joinCond.reduceLeftOption(And), JoinHint.NONE))
//  }
//}
//object RewriteExceptAll extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(EXCEPT), ruleId) {
//    case Except(left, right, true) if sentinel(true, "RewriteExceptAll.apply.case1") =>
//      Rule.coverage += "RewriteExceptAll.apply.case1"
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
//    case Intersect(left, right, true) if sentinel(true, "RewriteIntersectAll.apply.case1") =>
//      Rule.coverage += "RewriteIntersectAll.apply.case1"
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
//    case a @ Aggregate(grouping, _, _) if sentinel(true, "RemoveLiteralFromGroupExpressions.apply.case1") && grouping.nonEmpty =>
//      Rule.coverage += "RemoveLiteralFromGroupExpressions.apply.case1"
//      val newGrouping = grouping.filter(!_.foldable)
//      if (sentinel(true, "RemoveLiteralFromGroupExpressions.apply.then108") && newGrouping.nonEmpty) {
//        Rule.coverage += "RemoveLiteralFromGroupExpressions.apply.then108"
//        a.copy(groupingExpressions = newGrouping)
//      } else {
//        Rule.coverage += "RemoveLiteralFromGroupExpressions.apply.else109"
//        sentinel(true, "RemoveLiteralFromGroupExpressions.apply.else109")
//        a.copy(groupingExpressions = Seq(Literal(0, IntegerType)))
//      }
//  }
//}
//object GenerateOptimization extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformDownWithPruning(_.containsAllPatterns(PROJECT, GENERATE), ruleId) {
//    case p @ Project(_, g: Generate) if sentinel(true, "GenerateOptimization.apply.case1") && (p.references.isEmpty && (g.generator.isInstanceOf[ExplodeBase])) =>
//      Rule.coverage += "GenerateOptimization.apply.case1"
//      g.generator.children.head.dataType match {
//        case ArrayType(StructType(fields), containsNull) if sentinel(true, "GenerateOptimization.apply.match110") && fields.length > 1 =>
//          Rule.coverage += "GenerateOptimization.apply.match110"
//          val sortedFields = fields.zipWithIndex.sortBy(f => f._1.dataType.defaultSize)
//          val extractor = GetArrayStructFields(g.generator.children.head, sortedFields(0)._1, sortedFields(0)._2, fields.length, containsNull || sortedFields(0)._1.nullable)
//          val rewrittenG = g.transformExpressions {
//            case e: ExplodeBase if sentinel(true, "GenerateOptimization.apply.case112") =>
//              Rule.coverage += "GenerateOptimization.apply.case112"
//              e.withNewChildren(Seq(extractor))
//          }
//          val updatedGeneratorOutput = rewrittenG.generatorOutput.zip(toAttributes(rewrittenG.generator.elementSchema)).map {
//            case (oldAttr, newAttr) if sentinel(true, "GenerateOptimization.apply.case113") =>
//              Rule.coverage += "GenerateOptimization.apply.case113"
//              newAttr.withExprId(oldAttr.exprId).withName(oldAttr.name)
//          }
//          assert(updatedGeneratorOutput.length == rewrittenG.generatorOutput.length, "Updated generator output must have the same length " + "with original generator output.")
//          val updatedGenerate = rewrittenG.copy(generatorOutput = updatedGeneratorOutput)
//          p.withNewChildren(Seq(updatedGenerate))
//        case _ if sentinel(true, "GenerateOptimization.apply.match111") =>
//          Rule.coverage += "GenerateOptimization.apply.match111"
//          p
//      }
//  }
//}
//object RemoveRepetitionFromGroupExpressions extends Rule[LogicalPlan] {
//  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(AGGREGATE), ruleId) {
//    case a @ Aggregate(grouping, _, _) if sentinel(true, "RemoveRepetitionFromGroupExpressions.apply.case1") && grouping.size > 1 =>
//      Rule.coverage += "RemoveRepetitionFromGroupExpressions.apply.case1"
//      val newGrouping = ExpressionSet(grouping).toSeq
//      if (sentinel(true, "RemoveRepetitionFromGroupExpressions.apply.then114") && newGrouping.size == grouping.size) {
//        Rule.coverage += "RemoveRepetitionFromGroupExpressions.apply.then114"
//        a
//      } else {
//        Rule.coverage += "RemoveRepetitionFromGroupExpressions.apply.else115"
//        sentinel(true, "RemoveRepetitionFromGroupExpressions.apply.else115")
//        a.copy(groupingExpressions = newGrouping)
//      }
//  }
//}