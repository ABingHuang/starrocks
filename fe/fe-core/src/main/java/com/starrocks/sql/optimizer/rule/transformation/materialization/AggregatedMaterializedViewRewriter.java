// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * SPJG materialized view rewriter, based on
 * 《Optimizing Queries Using Materialized Views: A Practical, Scalable Solution》
 *
 *  This rewriter is for aggregated query rewrite
 */
public class AggregatedMaterializedViewRewriter extends MaterializedViewRewriter {
    private static final Logger LOG = LogManager.getLogger(AggregatedMaterializedViewRewriter.class);

    private static Map<String, String> ROLLUP_FUNCTION_MAP = ImmutableMap.<String, String>builder()
            .put(FunctionSet.COUNT, FunctionSet.SUM)
            .build();

    public AggregatedMaterializedViewRewriter(MaterializationContext materializationContext) {
        super(materializationContext);
    }

    @Override
    public boolean isValidPlan(OptExpression expression) {
        if (expression == null) {
            return false;
        }
        Operator op = expression.getOp();
        if (!(op instanceof LogicalAggregationOperator)) {
            return false;
        }
        LogicalAggregationOperator agg = (LogicalAggregationOperator) op;
        if (!agg.getType().equals(AggType.GLOBAL)) {
            return false;
        }
        // TODO: to support grouping set/rollup/cube
        return RewriteUtils.isLogicalSPJ(expression.inputAt(0));
    }

    @Override
    public OptExpression viewBasedRewrite(RewriteContext rewriteContext, OptExpression targetExpr) {
        LogicalAggregationOperator mvAgg = (LogicalAggregationOperator) rewriteContext.getMvExpression().getOp();
        List<ScalarOperator> swappedMvKeys = Lists.newArrayList();
        ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        for (ColumnRefOperator key : mvAgg.getGroupingKeys()) {
            ScalarOperator rewritten = rewriteContext.getMvColumnRefRewriter().rewrite(key.clone());
            ScalarOperator swapped = columnRewriter.rewriteViewToQueryWithQueryEc(rewritten);
            swappedMvKeys.add(swapped);
        }

        LogicalAggregationOperator queryAgg = (LogicalAggregationOperator) rewriteContext.getQueryExpression().getOp();
        List<ColumnRefOperator> originalGroupKeys = queryAgg.getGroupingKeys();
        List<ScalarOperator> queryGroupingKeys = Lists.newArrayList();
        for (ColumnRefOperator key : originalGroupKeys) {
            ScalarOperator rewritten = rewriteContext.getQueryColumnRefRewriter().rewrite(key.clone());
            ScalarOperator normalized = columnRewriter.rewriteByQueryEc(rewritten);
            queryGroupingKeys.add(normalized);
        }

        List<ScalarOperator> distinctMvKeys = swappedMvKeys.stream().distinct().collect(Collectors.toList());
        GroupKeyChecker groupKeyChecker = new GroupKeyChecker(distinctMvKeys);
        boolean keyMatched = groupKeyChecker.check(queryGroupingKeys);
        if (!keyMatched) {
            return null;
        }

        // check aggregates of query
        // normalize mv's aggs by using query's table ref and query ec
        List<ScalarOperator> swappedMvAggs = Lists.newArrayList();
        for (Map.Entry<ColumnRefOperator, CallOperator> aggEntry : mvAgg.getAggregations().entrySet()) {
            ScalarOperator rewritten = rewriteContext.getMvColumnRefRewriter().rewrite(aggEntry.getValue().clone());
            ScalarOperator swapped = columnRewriter.rewriteViewToQueryWithQueryEc(rewritten);
            swappedMvAggs.add(swapped);
        }

        Map<ColumnRefOperator, ScalarOperator> queryAggs = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> aggEntry : queryAgg.getAggregations().entrySet()) {
            ScalarOperator rewritten = rewriteContext.getQueryColumnRefRewriter().rewrite(aggEntry.getValue().clone());
            ScalarOperator swapped = columnRewriter.rewriteByQueryEc(rewritten);
            queryAggs.put(aggEntry.getKey(), swapped);
        }

        AggregateChecker aggregateChecker = new AggregateChecker(swappedMvAggs);
        boolean aggMatched = aggregateChecker.check(queryAggs.values().stream().collect(Collectors.toList()));
        if (!aggMatched) {
            return null;
        }
        Map<ColumnRefOperator, ScalarOperator> mvProjection = getProjectionMap(rewriteContext.getMvProjection(),
                rewriteContext.getMvExpression(), rewriteContext.getMvRefFactory());
        // normalize view projection by query relation and ec
        Multimap<ScalarOperator, ColumnRefOperator> normalizedViewMap =
                normalizeAndReverseProjection(mvProjection, rewriteContext, false);
        boolean isRollup = groupKeyChecker.isRollup();
        if (isRollup) {
            if (aggregateChecker.hasDistinct()) {
                // can not support rollup of disctinct agg
                return null;
            }
            // generate group keys against scan mv plan
            List<ColumnRefOperator> newGroupKeys = rewriteGroupKeys(
                    queryGroupingKeys, normalizedViewMap, rewriteContext.getOutputMapping(),
                    rewriteContext.getQueryColumnSet());
            if (newGroupKeys == null) {
                return null;
            }
            Preconditions.checkState(originalGroupKeys.size() == newGroupKeys.size());

            // generate new agg exprs(rollup functions)
            Map<ColumnRefOperator, CallOperator> newAggregations = rewriteAggregates(
                    queryAggs, normalizedViewMap, rewriteContext.getOutputMapping(),
                    rewriteContext.getQueryColumnSet());
            if (newAggregations == null) {
                return null;
            }
            // newGroupKeys may have duplicate because of EquivalenceClasses
            // remove duplicate here as new grouping keys
            List<ColumnRefOperator> finalGroupKeys = newGroupKeys.stream().distinct().collect(Collectors.toList());

            LogicalAggregationOperator.Builder aggBuilder = new LogicalAggregationOperator.Builder();
            aggBuilder.withOperator(queryAgg);
            aggBuilder.setGroupingKeys(finalGroupKeys);
            // can not be distinct agg here, so partitionByColumns is the same as groupingKeys
            aggBuilder.setPartitionByColumns(finalGroupKeys);
            aggBuilder.setAggregations(newAggregations);
            aggBuilder.setProjection(queryAgg.getProjection());
            aggBuilder.setPredicate(queryAgg.getPredicate());

            // add projection for group keys
            // aggregates are already mapped, so here just process group keys
            Map<ColumnRefOperator, ScalarOperator> newProjection = Maps.newHashMap();
            if (queryAgg.getProjection() == null) {
                for (int i = 0; i < originalGroupKeys.size(); i++) {
                    newProjection.put(originalGroupKeys.get(i), newGroupKeys.get(i));
                }
                newProjection.putAll(newAggregations);
            } else {
                Map<ColumnRefOperator, ScalarOperator> originalMap = queryAgg.getProjection().getColumnRefMap();
                Map<ColumnRefOperator, ScalarOperator> groupKeyMap = Maps.newHashMap();
                for (int i = 0; i < originalGroupKeys.size(); i++) {
                    groupKeyMap.put(originalGroupKeys.get(i), newGroupKeys.get(i));
                }
                ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(groupKeyMap);
                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : originalMap.entrySet()) {
                    if (groupKeyMap.containsKey(entry.getValue())) {
                        ScalarOperator rewritten = rewriter.rewrite(entry.getValue());
                        newProjection.put(entry.getKey(), rewritten);
                    } else {
                        newProjection.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            Projection projection = new Projection(newProjection);
            aggBuilder.setProjection(projection);
            LogicalAggregationOperator newAggOp = aggBuilder.build();
            OptExpression aggExpr = OptExpression.create(newAggOp, targetExpr);
            return aggExpr;
        } else {
            return rewriteProjection(rewriteContext, normalizedViewMap, targetExpr);
        }
    }

    private List<ColumnRefOperator> rewriteGroupKeys(List<ScalarOperator> groupKeys,
                                                     Multimap<ScalarOperator, ColumnRefOperator> normalizedViewMap,
                                                     Map<ColumnRefOperator, ColumnRefOperator> mapping,
                                                     Set<ColumnRefOperator> queryColumnSet) {
        List<ColumnRefOperator> rewrittens = Lists.newArrayList();
        for (ScalarOperator key : groupKeys) {
            ScalarOperator targetColumn = replaceExprWithTarget(key, normalizedViewMap, mapping);
            if (!isAllExprReplaced(targetColumn, queryColumnSet)) {
                // it means there is some column that can not be rewritten by outputs of mv
                return null;
            }
            rewrittens.add((ColumnRefOperator) targetColumn);
        }
        return rewrittens;
    }

    private Map<ColumnRefOperator, CallOperator> rewriteAggregates(Map<ColumnRefOperator, ScalarOperator> aggregates,
                                                           Multimap<ScalarOperator, ColumnRefOperator> normalizedViewMap,
                                                           Map<ColumnRefOperator, ColumnRefOperator> mapping,
                                                           Set<ColumnRefOperator> queryColumnSet) {
        Map<ColumnRefOperator, CallOperator> rewrittens = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : aggregates.entrySet()) {
            Preconditions.checkState(entry.getValue() instanceof CallOperator);
            CallOperator aggCall = (CallOperator) entry.getValue();
            ScalarOperator targetColumn = replaceExprWithTarget(aggCall, normalizedViewMap, mapping);
            if (!isAllExprReplaced(targetColumn, queryColumnSet)) {
                // it means there is some column that can not be rewritten by outputs of mv
                return null;
            }
            // Aggregate must be CallOperator
            Preconditions.checkState(targetColumn instanceof ColumnRefOperator);
            CallOperator newAggregate = getRollupAggregate(aggCall, (ColumnRefOperator) targetColumn);
            if (newAggregate == null) {
                return null;
            }
            rewrittens.put(entry.getKey(), newAggregate);
        }

        return rewrittens;
    }

    // generate new aggregates for rollup
    // eg: count(col) -> sum(col)
    private CallOperator getRollupAggregate(CallOperator aggCall, ColumnRefOperator targetColumn) {
        if (ROLLUP_FUNCTION_MAP.containsKey(aggCall.getFnName())) {
            if (aggCall.getFnName().equals(FunctionSet.COUNT)) {
                Function sumFn = findArithmeticFunction(aggCall.getFunction().getArgs(), FunctionSet.SUM);
                return new CallOperator(FunctionSet.SUM, aggCall.getFunction().getReturnType(),
                        Lists.newArrayList(targetColumn), sumFn);
            } else {
                // impossible to reach here
                LOG.warn("unsupported rollup function:{}", aggCall.getFnName());
                return null;
            }
        } else {
            // the rollup funcation is the same as origin, but use the new column as argument
            CallOperator newAggCall = (CallOperator) aggCall.clone();
            newAggCall.setChild(0, targetColumn);
            return newAggCall;
        }
    }

    private Function findArithmeticFunction(Type[] argsType, String fnName) {
        return Expr.getBuiltinFunction(fnName, argsType, Function.CompareMode.IS_IDENTICAL);
    }
}
