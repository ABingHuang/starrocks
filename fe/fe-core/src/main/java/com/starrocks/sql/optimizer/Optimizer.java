// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.Explain;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.cost.CostEstimate;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.rule.join.ReorderJoinRule;
import com.starrocks.sql.optimizer.rule.mv.MaterializedViewRule;
import com.starrocks.sql.optimizer.rule.transformation.ApplyExceptionRule;
import com.starrocks.sql.optimizer.rule.transformation.GroupByCountDistinctRewriteRule;
import com.starrocks.sql.optimizer.rule.transformation.LimitPruneTabletsRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeProjectWithChildRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoAggRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneEmptyWindowRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownAggToMetaScanRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownJoinOnExpressionToChildProject;
import com.starrocks.sql.optimizer.rule.transformation.PushDownLimitRankingWindowRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateRankingWindowRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownProjectLimitRule;
import com.starrocks.sql.optimizer.rule.transformation.PushLimitAndFilterToCTEProduceRule;
import com.starrocks.sql.optimizer.rule.transformation.RemoveAggregationFromAggTable;
import com.starrocks.sql.optimizer.rule.transformation.ReorderIntersectRule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteGroupingSetsByCTERule;
import com.starrocks.sql.optimizer.rule.transformation.SemiReorderRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MaterializationContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.RewriteUtils;
import com.starrocks.sql.optimizer.rule.tree.AddDecodeNodeForDictStringRule;
import com.starrocks.sql.optimizer.rule.tree.ExchangeSortToMergeRule;
import com.starrocks.sql.optimizer.rule.tree.PreAggregateTurnOnRule;
import com.starrocks.sql.optimizer.rule.tree.PredicateReorderRule;
import com.starrocks.sql.optimizer.rule.tree.PruneAggregateNodeRule;
import com.starrocks.sql.optimizer.rule.tree.PruneShuffleColumnRule;
import com.starrocks.sql.optimizer.rule.tree.PushDownAggregateRule;
import com.starrocks.sql.optimizer.rule.tree.ScalarOperatorsReuseRule;
import com.starrocks.sql.optimizer.rule.tree.UseSortAggregateRule;
import com.starrocks.sql.optimizer.task.OptimizeGroupTask;
import com.starrocks.sql.optimizer.task.RewriteTreeTask;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.parser.ParsingException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Optimizer's entrance class
 */
public class Optimizer {
    private static final Logger LOG = LogManager.getLogger(Optimizer.class);
    private final OptimizerConfig optimizerConfig;
    private OptimizerContext context;

    public Optimizer() {
        this(OptimizerConfig.defaultConfig());
    }

    public Optimizer(OptimizerConfig config) {
        this.optimizerConfig = config;
    }

    public OptimizerContext getContext() {
        return context;
    }

    public OptExpression optimize(ConnectContext connectContext,
                                  OptExpression logicOperatorTree,
                                  PhysicalPropertySet requiredProperty,
                                  ColumnRefSet requiredColumns,
                                  ColumnRefFactory columnRefFactory) {
        prepare(connectContext, logicOperatorTree, columnRefFactory);
        if (optimizerConfig.isRuleBased()) {
            return optimizeByRule(connectContext, logicOperatorTree, requiredProperty, requiredColumns);
        } else {
            return optimizeByCost(connectContext, logicOperatorTree, requiredProperty, requiredColumns);
        }
    }

    private OptExpression optimizeByRule(ConnectContext connectContext,
                                        OptExpression logicOperatorTree,
                                        PhysicalPropertySet requiredProperty,
                                        ColumnRefSet requiredColumns) {
        // Phase 1: none
        OptimizerTraceUtil.logOptExpression(connectContext, "origin logicOperatorTree:\n%s", logicOperatorTree);
        TaskContext rootTaskContext =
                new TaskContext(context, requiredProperty, requiredColumns.clone(), Double.MAX_VALUE);
        logicOperatorTree = logicalRuleRewrite(logicOperatorTree, rootTaskContext);
        OptimizerTraceUtil.log(connectContext, "after logical rewrite, new logicOperatorTree:\n%s", logicOperatorTree);
        return logicOperatorTree;
    }

    /**
     * Optimizer will transform and implement the logical operator based on
     * the {@see Rule}, then cost the physical operator, and finally find the
     * lowest cost physical operator tree
     *
     * @param logicOperatorTree the input for query Optimizer
     * @param requiredProperty  the required physical property from sql or groupExpression
     * @param requiredColumns   the required output columns from sql or groupExpression
     * @return the lowest cost physical operator for this query
     */
    private OptExpression optimizeByCost(ConnectContext connectContext,
                                  OptExpression logicOperatorTree,
                                  PhysicalPropertySet requiredProperty,
                                  ColumnRefSet requiredColumns) {
        // Phase 1: none
        OptimizerTraceUtil.logOptExpression(connectContext, "origin logicOperatorTree:\n%s", logicOperatorTree);
        Memo memo = context.getMemo();
        TaskContext rootTaskContext =
                new TaskContext(context, requiredProperty, requiredColumns.clone(), Double.MAX_VALUE);

        logicOperatorTree = logicalRuleRewrite(logicOperatorTree, rootTaskContext);

        memo.init(logicOperatorTree);
        OptimizerTraceUtil.log(connectContext, "after logical rewrite, root group:\n%s", memo.getRootGroup());

        // collect all olap scan operator
        collectAllScanOperators(memo, rootTaskContext);

        // Currently, we cache output columns in logic property.
        // We derive logic property Bottom Up firstly when new group added to memo,
        // but we do column prune rewrite top down later.
        // So after column prune rewrite, the output columns for each operator maybe change,
        // but the logic property is cached and never change.
        // So we need to explicitly derive all group logic property again
        memo.deriveAllGroupLogicalProperty();

        // Phase 3: optimize based on memo and group
        memoOptimize(connectContext, memo, rootTaskContext);

        OptExpression result;
        if (!connectContext.getSessionVariable().isSetUseNthExecPlan()) {
            result = extractBestPlan(requiredProperty, memo.getRootGroup());
        } else {
            // extract the nth execution plan
            int nthExecPlan = connectContext.getSessionVariable().getUseNthExecPlan();
            result = EnumeratePlan.extractNthPlan(requiredProperty, memo.getRootGroup(), nthExecPlan);
        }
        OptimizerTraceUtil.logOptExpression(connectContext, "after extract best plan:\n%s", result);

        // set costs audio log before physicalRuleRewrite
        // statistics won't set correctly after physicalRuleRewrite.
        // we need set plan costs before physical rewrite stage.
        final CostEstimate costs = Explain.buildCost(result);
        connectContext.getAuditEventBuilder().setPlanCpuCosts(costs.getCpuCost())
                .setPlanMemCosts(costs.getMemoryCost());

        OptExpression finalPlan = physicalRuleRewrite(rootTaskContext, result);
        OptimizerTraceUtil.logOptExpression(connectContext, "final plan after physical rewrite:\n%s", finalPlan);
        OptimizerTraceUtil.log(connectContext, context.getTraceInfo());
        return finalPlan;
    }

    private OptExpression logicalRuleRewrite(OptExpression tree, TaskContext rootTaskContext) {
        tree = OptExpression.create(new LogicalTreeAnchorOperator(), tree);
        ColumnRefSet requiredColumns = rootTaskContext.getRequiredColumns().clone();
        deriveLogicalProperty(tree);

        SessionVariable sessionVariable = rootTaskContext.getOptimizerContext().getSessionVariable();
        CTEContext cteContext = context.getCteContext();
        CTEUtils.collectCteOperators(tree, context);
        // inline CTE if consume use once
        while (cteContext.hasInlineCTE()) {
            ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.INLINE_CTE);
            CTEUtils.collectCteOperators(tree, context);
        }

        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.AGGREGATE_REWRITE);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PUSH_DOWN_SUBQUERY);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.SUBQUERY_REWRITE_COMMON);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.SUBQUERY_REWRITE_TO_WINDOW);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.SUBQUERY_REWRITE_TO_JOIN);
        ruleRewriteOnlyOnce(tree, rootTaskContext, new ApplyExceptionRule());
        CTEUtils.collectCteOperators(tree, context);

        // Note: PUSH_DOWN_PREDICATE tasks should be executed before MERGE_LIMIT tasks
        // because of the Filter node needs to be merged first to avoid the Limit node
        // cannot merge
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PUSH_DOWN_PREDICATE);

        ruleRewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());
        ruleRewriteOnlyOnce(tree, rootTaskContext, new PushDownAggToMetaScanRule());
        ruleRewriteOnlyOnce(tree, rootTaskContext, new PushDownPredicateRankingWindowRule());
        ruleRewriteOnlyOnce(tree, rootTaskContext, new PushDownJoinOnExpressionToChildProject());
        ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PRUNE_COLUMNS);
        deriveLogicalProperty(tree);

        ruleRewriteIterative(tree, rootTaskContext, new PruneEmptyWindowRule());
        // @todo: resolve recursive optimization question:
        //  MergeAgg -> PruneColumn -> PruneEmptyWindow -> MergeAgg/Project -> PruneColumn...
        ruleRewriteIterative(tree, rootTaskContext, new MergeTwoAggRule());
        rootTaskContext.setRequiredColumns(requiredColumns.clone());
        ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PRUNE_COLUMNS);

        ruleRewriteIterative(tree, rootTaskContext, new PruneEmptyWindowRule());
        ruleRewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());
        //Limit push must be after the column prune,
        //otherwise the Node containing limit may be prune
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.MERGE_LIMIT);
        ruleRewriteIterative(tree, rootTaskContext, new PushDownProjectLimitRule());

        ruleRewriteOnlyOnce(tree, rootTaskContext, new PushDownLimitRankingWindowRule());
        if (sessionVariable.isEnableRewriteGroupingsetsToUnionAll()) {
            ruleRewriteIterative(tree, rootTaskContext, new RewriteGroupingSetsByCTERule());
        }

        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PRUNE_ASSERT_ROW);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PRUNE_PROJECT);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PRUNE_SET_OPERATOR);

        CTEUtils.collectCteOperators(tree, context);
        if (cteContext.needOptimizeCTE()) {
            cteContext.reset();
            ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.COLLECT_CTE);
            rootTaskContext.setRequiredColumns(requiredColumns.clone());
            ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PRUNE_COLUMNS);
            if (cteContext.needPushLimit() || cteContext.needPushPredicate()) {
                ruleRewriteOnlyOnce(tree, rootTaskContext, new PushLimitAndFilterToCTEProduceRule());
            }

            if (cteContext.needPushPredicate()) {
                ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PUSH_DOWN_PREDICATE);
            }

            if (cteContext.needPushLimit()) {
                ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.MERGE_LIMIT);
            }
        }

        tree = new MaterializedViewRule().transform(tree, context).get(0);
        deriveLogicalProperty(tree);

        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.MULTI_DISTINCT_REWRITE);
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PUSH_DOWN_PREDICATE);

        ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PARTITION_PRUNE);
        ruleRewriteOnlyOnce(tree, rootTaskContext, LimitPruneTabletsRule.getInstance());
        ruleRewriteIterative(tree, rootTaskContext, RuleSetType.PRUNE_PROJECT);

        tree = pushDownAggregation(tree, rootTaskContext, requiredColumns);

        CTEUtils.collectCteOperators(tree, context);
        // inline CTE if consume use once
        while (cteContext.hasInlineCTE()) {
            ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.INLINE_CTE);
            CTEUtils.collectCteOperators(tree, context);
        }

        ruleRewriteIterative(tree, rootTaskContext, new PruneEmptyWindowRule());
        ruleRewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());
        ruleRewriteIterative(tree, rootTaskContext, new MergeProjectWithChildRule());
        ruleRewriteOnlyOnce(tree, rootTaskContext, new GroupByCountDistinctRewriteRule());
        ruleRewriteOnlyOnce(tree, rootTaskContext, new ReorderIntersectRule());
        ruleRewriteIterative(tree, rootTaskContext, new RemoveAggregationFromAggTable());

        if (optimizerConfig.isEnableMvRewrite() && sessionVariable.isEnableRuleBasedMaterializedViewRewrite()) {
            // now add single table materialized view rewrite rules in rule based rewrite phase to boost optimization
            ruleRewriteIterative(tree, rootTaskContext, RuleSetType.SINGLE_TABLE_MV_REWRITE);
        }

        return tree.getInputs().get(0);
    }


    private OptExpression pushDownAggregation(OptExpression tree, TaskContext rootTaskContext,
                                              ColumnRefSet requiredColumns) {
        if (context.getSessionVariable().getCboPushDownAggregateMode() == -1) {
            return tree;
        }

        tree = new PushDownAggregateRule().rewrite(tree, rootTaskContext);
        deriveLogicalProperty(tree);

        rootTaskContext.setRequiredColumns(requiredColumns.clone());
        ruleRewriteOnlyOnce(tree, rootTaskContext, RuleSetType.PRUNE_COLUMNS);
        return tree;
    }

    private void prepare(ConnectContext connectContext, OptExpression logicOperatorTree, ColumnRefFactory columnRefFactory) {
        Memo memo = null;
        if (!optimizerConfig.isRuleBased()) {
            memo = new Memo();
        }

        context = new OptimizerContext(memo, columnRefFactory, connectContext);
        context.setTraceInfo(new OptimizerTraceInfo(connectContext.getQueryId()));

        // process materialized views
        if (Config.enable_experimental_mv && optimizerConfig.isEnableMvRewrite()
                && connectContext.getSessionVariable().isEnableMaterializedViewRewrite()) {
            // register materialized views
            registerMaterializedViews(logicOperatorTree, connectContext);
        }
    }

    private void deriveLogicalProperty(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            deriveLogicalProperty(child);
        }

        ExpressionContext context = new ExpressionContext(root);
        context.deriveLogicalProperty();
        root.setLogicalProperty(context.getRootProperty());
    }

    void memoOptimize(ConnectContext connectContext, Memo memo, TaskContext rootTaskContext) {
        OptExpression tree = memo.getRootGroup().extractLogicalTree();
        // Join reorder
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        if (!sessionVariable.isDisableJoinReorder()
                && Utils.countInnerJoinNodeSize(tree) < sessionVariable.getCboMaxReorderNode()) {
            if (Utils.countInnerJoinNodeSize(tree) > sessionVariable.getCboMaxReorderNodeUseExhaustive()) {
                CTEUtils.collectForceCteStatistics(memo, context);
                new ReorderJoinRule().transform(tree, context);
                context.getRuleSet().addJoinCommutativityWithOutInnerRule();
            } else {
                if (Utils.capableSemiReorder(tree, false, 0, sessionVariable.getCboMaxReorderNodeUseExhaustive())) {
                    context.getRuleSet().getTransformRules().add(new SemiReorderRule());
                }
                context.getRuleSet().addJoinTransformationRules();
            }
        }

        //add join implementRule
        String joinImplementationMode = ConnectContext.get().getSessionVariable().getJoinImplementationMode();
        if ("merge".equalsIgnoreCase(joinImplementationMode)) {
            context.getRuleSet().addMergeJoinImplementationRule();
        } else if ("hash".equalsIgnoreCase(joinImplementationMode)) {
            context.getRuleSet().addHashJoinImplementationRule();
        } else if ("nestloop".equalsIgnoreCase(joinImplementationMode)) {
            context.getRuleSet().addNestLoopJoinImplementationRule();
        } else {
            context.getRuleSet().addAutoJoinImplementationRule();
        }

        // TODO: should add session variable to control these rewrite rule
        if (!context.getCandidateMvs().isEmpty()) {
            context.getRuleSet().addMultiTableMvRewriteRule();
        }

        context.getTaskScheduler().pushTask(new OptimizeGroupTask(rootTaskContext, memo.getRootGroup()));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }

    private OptExpression physicalRuleRewrite(TaskContext rootTaskContext, OptExpression result) {
        Preconditions.checkState(result.getOp().isPhysical());

        // Since there may be many different plans in the logic phase, it's possible
        // that this switch can't turned on after logical optimization, so we only determine
        // whether the PreAggregate can be turned on in the final
        result = new PreAggregateTurnOnRule().rewrite(result, rootTaskContext);

        // Rewrite Exchange on top of Sort to Final Sort
        result = new ExchangeSortToMergeRule().rewrite(result, rootTaskContext);
        result = new PruneAggregateNodeRule().rewrite(result, rootTaskContext);
        result = new PruneShuffleColumnRule().rewrite(result, rootTaskContext);
        result = new AddDecodeNodeForDictStringRule().rewrite(result, rootTaskContext);
        // This rule should be last
        result = new ScalarOperatorsReuseRule().rewrite(result, rootTaskContext);
        // Reorder predicates
        result = new PredicateReorderRule(rootTaskContext.getOptimizerContext().getSessionVariable()).rewrite(result,
                rootTaskContext);
        result = new UseSortAggregateRule().rewrite(result, rootTaskContext);
        return result;
    }

    /**
     * Extract the lowest cost physical operator tree from memo
     *
     * @param requiredProperty the required physical property from sql or groupExpression
     * @param rootGroup        the current group to find the lowest cost physical operator
     * @return the lowest cost physical operator for this query
     */
    private OptExpression extractBestPlan(PhysicalPropertySet requiredProperty,
                                          Group rootGroup) {
        GroupExpression groupExpression = rootGroup.getBestExpression(requiredProperty);
        Preconditions.checkNotNull(groupExpression, "no executable plan for this sql");
        List<PhysicalPropertySet> inputProperties = groupExpression.getInputProperties(requiredProperty);

        List<OptExpression> childPlans = Lists.newArrayList();
        for (int i = 0; i < groupExpression.arity(); ++i) {
            OptExpression childPlan = extractBestPlan(inputProperties.get(i), groupExpression.inputAt(i));
            childPlans.add(childPlan);
        }

        OptExpression expression = OptExpression.create(groupExpression.getOp(),
                childPlans);
        // record inputProperties at optExpression, used for planFragment builder to determine join type
        expression.setRequiredProperties(inputProperties);
        expression.setOutputProperty(groupExpression.getOutputProperty(requiredProperty));
        expression.setStatistics(groupExpression.getGroup().hasConfidenceStatistic(requiredProperty) ?
                groupExpression.getGroup().getConfidenceStatistic(requiredProperty) :
                groupExpression.getGroup().getStatistics());
        expression.setCost(groupExpression.getCost(requiredProperty));

        // When build plan fragment, we need the output column of logical property
        expression.setLogicalProperty(rootGroup.getLogicalProperty());
        return expression;
    }

    private void collectAllScanOperators(Memo memo, TaskContext rootTaskContext) {
        OptExpression tree = memo.getRootGroup().extractLogicalTree();
        List<LogicalOlapScanOperator> list = Lists.newArrayList();
        Utils.extractOlapScanOperator(tree.getGroupExpression(), list);
        rootTaskContext.setAllScanOperators(Collections.unmodifiableList(list));
    }

    private void ruleRewriteIterative(OptExpression tree, TaskContext rootTaskContext, RuleSetType ruleSetType) {
        List<Rule> rules = rootTaskContext.getOptimizerContext().getRuleSet().getRewriteRulesByType(ruleSetType);
        context.getTaskScheduler().pushTask(new RewriteTreeTask(rootTaskContext, tree, rules, false));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }

    private void ruleRewriteIterative(OptExpression tree, TaskContext rootTaskContext, Rule rule) {
        List<Rule> rules = Collections.singletonList(rule);
        context.getTaskScheduler().pushTask(new RewriteTreeTask(rootTaskContext, tree, rules, false));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }

    private void ruleRewriteOnlyOnce(OptExpression tree, TaskContext rootTaskContext, RuleSetType ruleSetType) {
        List<Rule> rules = rootTaskContext.getOptimizerContext().getRuleSet().getRewriteRulesByType(ruleSetType);
        context.getTaskScheduler().pushTask(new RewriteTreeTask(rootTaskContext, tree, rules, true));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }

    private void ruleRewriteOnlyOnce(OptExpression tree, TaskContext rootTaskContext, Rule rule) {
        List<Rule> rules = Collections.singletonList(rule);
        context.getTaskScheduler().pushTask(new RewriteTreeTask(rootTaskContext, tree, rules, true));
        context.getTaskScheduler().executeTasks(rootTaskContext);
    }

    // get nested mvs by getting recursively
    void getRelatedMvs(List<Table> tablesToCheck, Set<MaterializedView> mvs) {
        Set<Table.MaterializedViewId> newMvIds = Sets.newHashSet();
        for (Table table : tablesToCheck) {
            Set<Table.MaterializedViewId> mvIds = table.getRelatedMaterializedViews();
            if (mvIds != null && !mvIds.isEmpty()) {
                newMvIds.addAll(mvIds);
            }
        }
        if (newMvIds.isEmpty()) {
            return;
        }
        List<Table> newMvs = Lists.newArrayList();
        for (Table.MaterializedViewId mvId : newMvIds) {
            Database db = context.getCatalog().getDb(mvId.getDbId());
            if (db == null) {
                continue;
            }
            Table table = db.getTable(mvId.getMvId());
            if (table == null) {
                continue;
            }
            newMvs.add(table);
            mvs.add((MaterializedView) table);
        }
        getRelatedMvs(newMvs, mvs);
    }

    private Pair<OptExpression, LogicalPlan> getOptimizedLogicalPlan(String sql,
                                                                     ColumnRefFactory columnRefFactory,
                                                                     ConnectContext connectContext) {
        StatementBase mvStmt;
        try {
            mvStmt = com.starrocks.sql.parser.SqlParser.parseSingleSql(sql, context.getSessionVariable());
        } catch (ParsingException parsingException) {
            LOG.warn("parse sql:{} failed", sql, parsingException);
            return null;
        }
        Preconditions.checkState(mvStmt instanceof QueryStatement);
        Analyzer.analyze(mvStmt, connectContext);
        QueryRelation query = ((QueryStatement) mvStmt).getQueryRelation();
        LogicalPlan logicalPlan =
                new RelationTransformer(columnRefFactory, connectContext).transformWithSelectLimit(query);
        // optimize the sql by rule and disable rule based materialized view rewrite
        OptimizerConfig optimizerConfig = new OptimizerConfig(OptimizerConfig.OptimizerAlgorithm.RULE_BASED, false);
        Optimizer optimizer = new Optimizer(optimizerConfig);
        OptExpression optimizedPlan = optimizer.optimize(
                connectContext,
                logicalPlan.getRoot(),
                new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()),
                columnRefFactory);
        return Pair.create(optimizedPlan, logicalPlan);
    }

    private void registerMaterializedViews(OptExpression logicOperatorTree, ConnectContext connectContext) {
        List<Table> tables = RewriteUtils.getAllTables(logicOperatorTree);

        // include nested materialized views
        Set<MaterializedView> relatedMvs = Sets.newHashSet();
        getRelatedMvs(tables, relatedMvs);

        for (MaterializedView mv : relatedMvs) {
            if (!mv.isActive()) {
                continue;
            }
            Set<String> partitionNamesToRefresh = Sets.newHashSet();

            /*
            Set<String> partitionNamesToRefresh = mv.getPartitionNamesToRefresh();
            PartitionInfo partitionInfo = mv.getPartitionInfo();
            if (partitionInfo instanceof SinglePartitionInfo) {
                if (!partitionNamesToRefresh.isEmpty()) {
                    continue;
                }
            } else if (partitionNamesToRefresh.containsAll(mv.getPartitionNames())) {
                // if the mv is partitioned, and all partitions need refresh,
                // then it can not be candidate
                continue;
            }

             */

            // 1. build mv query logical plan
            String mvSql = mv.getViewDefineSql();
            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            Pair<OptExpression, LogicalPlan> plans = getOptimizedLogicalPlan(mvSql, columnRefFactory, connectContext);
            if (plans == null) {
                continue;
            }
            OptExpression optimizedPlan = plans.first;
            if (!isValidSPJGPlan(optimizedPlan)) {
                continue;
            }
            if (mv.getPartitionInfo() instanceof ExpressionRangePartitionInfo && !partitionNamesToRefresh.isEmpty()) {
                updatePartialPartitionPredicate(mv, columnRefFactory, partitionNamesToRefresh, optimizedPlan);
            }

            List<ColumnRefOperator> outputExpressions = plans.second.getOutputColumn();
            MaterializationContext materializationContext = new MaterializationContext(mv, optimizedPlan,
                    columnRefFactory, outputExpressions, partitionNamesToRefresh);

            // generate scan mv plan
            Database db = context.getCatalog().getDb(mv.getDbId());
            TableName tableName = new TableName(db.getFullName(), mv.getName());
            String selectMvSql = "select * from " + tableName.toSql();
            Pair<OptExpression, LogicalPlan> mvPlans =
                    getOptimizedLogicalPlan(selectMvSql, context.getColumnRefFactory(), connectContext);
            OptExpression mvOptimizedPlan = mvPlans.first;
            if (!RewriteUtils.isLogicalSPJ(mvOptimizedPlan)) {
                continue;
            }
            if (!(mvOptimizedPlan.getOp() instanceof LogicalOlapScanOperator)) {
                continue;
            }
            materializationContext.setScanMvOperator(mvOptimizedPlan.getOp());
            List<ColumnRefOperator> mvOutputExpressions = mvPlans.second.getOutputColumn();
            materializationContext.setScanMvOutputExpressions(mvOutputExpressions);

            context.addCandidateMvs(materializationContext);
        }
    }

    private void updatePartialPartitionPredicate(MaterializedView mv, ColumnRefFactory columnRefFactory,
                                                 Set<String> partitionsToRefresh, OptExpression mvPlan) {
        // to support partial partition rewrite
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (!(partitionInfo instanceof ExpressionRangePartitionInfo)) {
            return;
        }
        ExpressionRangePartitionInfo exprPartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
        if (partitionsToRefresh.isEmpty()) {
            // all partitions are uptodate, do not add filter
            return;
        }

        Set<Long> outdatePartitionIds = Sets.newHashSet();
        for (Partition partition : mv.getPartitions()) {
            if (partitionsToRefresh.contains(partition.getName())) {
                outdatePartitionIds.add(partition.getId());
            }
        }
        if (outdatePartitionIds.size() == mv.getPartitions().size()) {
            // all partitions are out of date
            // should not reach here, it will be filtered when registering mv
            return;
        }
        // now only one column is supported
        Column partitionColumn = exprPartitionInfo.getPartitionColumns().get(0);
        List<Range<PartitionKey>> uptodatePartitionRanges = exprPartitionInfo.getRangeList(outdatePartitionIds, false);
        if (uptodatePartitionRanges.isEmpty()) {
            return;
        }
        List<Range<PartitionKey>> finalRanges = Lists.newArrayList();
        for (int i = 0; i < uptodatePartitionRanges.size(); i++) {
            Range<PartitionKey> currentRange = uptodatePartitionRanges.get(i);
            for (int j = 0; j < finalRanges.size(); j++) {
                // 1 < r < 10, 10 <= r < 20 => 1 < r < 20
                Range<PartitionKey> resultRange = finalRanges.get(j);
                if (currentRange.isConnected(currentRange) && currentRange.intersection(resultRange).isEmpty()) {
                    finalRanges.set(j, resultRange.span(currentRange));
                }
            }
        }
        // convert finalRanges into ScalarOperator
        List<MaterializedView.BaseTableInfo> baseTables = mv.getBaseTableInfos();
        Expr partitionExpr = exprPartitionInfo.getPartitionExprs().get(0);
        Pair<Table, Column> partitionTableAndColumns = getPartitionTableAndColumn(partitionExpr, baseTables);
        if (partitionTableAndColumns == null) {
            return;
        }
        List<OptExpression> scanExprs = collectScanExprs(mvPlan);
        for (OptExpression scanExpr : scanExprs) {
            LogicalScanOperator scanOperator = (LogicalScanOperator) scanExpr.getOp();
            Table scanTable = scanOperator.getTable();
            if ((scanTable.isLocalTable() && !scanTable.equals(partitionTableAndColumns.first))
                    || (!scanTable.isLocalTable()) && !scanTable.getTableIdentifier().equals(
                    partitionTableAndColumns.first.getTableIdentifier())) {
                continue;
            }
            ColumnRefOperator columnRef = scanOperator.getColumnReference(partitionColumn);
            ExpressionMapping expressionMapping =
                    new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()),
                            Lists.newArrayList());
            expressionMapping.put(partitionColumn.getRefColumn(), columnRef);
            // convert partition expr into partition scalar operator
            ScalarOperator partitionScalar = SqlToScalarOperatorTranslator.translate(partitionExpr,
                    expressionMapping, columnRefFactory);
            List<ScalarOperator> partitionPredicates = convertRanges(partitionScalar, finalRanges);
            ScalarOperator partitionPredicate = Utils.compoundOr(partitionPredicates);
            // here can directly change the plan of mv
            scanOperator.setPredicate(partitionPredicate);
        }
    }

    List<ScalarOperator> convertRanges(ScalarOperator partitionScalar, List<Range<PartitionKey>> partitionRanges) {
        List<ScalarOperator> rangeParts = Lists.newArrayList();
        for (Range<PartitionKey> range : partitionRanges) {
            if (range.isEmpty()) {
                continue;
            }
            if (range.hasLowerBound() && range.hasUpperBound()) {
                // close, open range
                ConstantOperator lowerBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.lowerEndpoint().getKeys().get(0));
                BinaryPredicateOperator lowerPredicate = new BinaryPredicateOperator(
                        BinaryPredicateOperator.BinaryType.GE, partitionScalar, lowerBound);

                ConstantOperator upperBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.upperEndpoint().getKeys().get(0));
                BinaryPredicateOperator upperPredicate = new BinaryPredicateOperator(
                        BinaryPredicateOperator.BinaryType.LT, partitionScalar, upperBound);

                CompoundPredicateOperator andPredicate = new CompoundPredicateOperator(
                        CompoundPredicateOperator.CompoundType.AND, lowerPredicate, upperPredicate);
                rangeParts.add(andPredicate);
            } else if (range.hasUpperBound()) {
                ConstantOperator upperBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.upperEndpoint().getKeys().get(0));
                BinaryPredicateOperator upperPredicate = new BinaryPredicateOperator(
                        BinaryPredicateOperator.BinaryType.LT, partitionScalar, upperBound);
                rangeParts.add(upperPredicate);
            } else if (range.hasLowerBound()) {
                ConstantOperator lowerBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.lowerEndpoint().getKeys().get(0));
                BinaryPredicateOperator lowerPredicate = new BinaryPredicateOperator(
                        BinaryPredicateOperator.BinaryType.GE, partitionScalar, lowerBound);
                rangeParts.add(lowerPredicate);
            } else {
                LOG.warn("impossible to reach here");
            }
        }
        return rangeParts;
    }

    List<OptExpression> collectScanExprs(OptExpression expression) {
        List<OptExpression> scanExprs = Lists.newArrayList();
        OptExpressionVisitor scanCollector = new OptExpressionVisitor<Void, Void>() {
            @Override
            public Void visit(OptExpression optExpression, Void context) {
                for (OptExpression input : optExpression.getInputs()) {
                    super.visit(input, context);
                }
                return super.visit(optExpression, context);
            }

            @Override
            public Void visitLogicalTableScan(OptExpression optExpression, Void context) {
                scanExprs.add(optExpression);
                return null;
            }
        };
        expression.getOp().accept(scanCollector, expression, null);
        return scanExprs;
    }

    private Pair<Table, Column> getPartitionTableAndColumn(Expr partitionExpr,
                                                           List<MaterializedView.BaseTableInfo> baseTables) {
        List<SlotRef> slotRefs = com.clearspring.analytics.util.Lists.newArrayList();
        partitionExpr.collect(SlotRef.class, slotRefs);
        // if partitionExpr is FunctionCallExpr, get first SlotRef
        Preconditions.checkState(slotRefs.size() == 1);
        SlotRef slotRef = slotRefs.get(0);
        for (MaterializedView.BaseTableInfo tableInfo : baseTables) {
            if (slotRef.getTblNameWithoutAnalyzed().getTbl().equals(tableInfo.getTableName())) {
                Table table = tableInfo.getTable();
                return Pair.create(table, table.getColumn(slotRef.getColumnName()));
            }
        }
        return null;
    }

    private boolean isValidSPJGPlan(OptExpression plan) {
        Operator op = plan.getOp();
        Preconditions.checkState(op instanceof LogicalOperator);
        if (op instanceof LogicalAggregationOperator) {
            // Aggregate - SPJ
            return RewriteUtils.isLogicalSPJG(plan);
        } else if (op instanceof LogicalProjectOperator) {
            if (plan.inputAt(0).getOp() instanceof LogicalAggregationOperator) {
                // Project - Aggregate - SPJ
                OptExpression aggExpr = plan.inputAt(0);
                return RewriteUtils.isLogicalSPJG(aggExpr);
            } else {
                // Projection - SPJ
                return RewriteUtils.isLogicalSPJ(plan.inputAt(0));
            }
        } else {
            return RewriteUtils.isLogicalSPJ(plan);
        }
    }
}
