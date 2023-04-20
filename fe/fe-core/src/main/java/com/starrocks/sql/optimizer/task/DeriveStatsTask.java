// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.task;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

/**
 * DeriveStatsTask derives any stats needed for costing a GroupExpression.
 */
public class DeriveStatsTask extends OptimizerTask {
    private final GroupExpression groupExpression;

    public DeriveStatsTask(TaskContext context, GroupExpression expression) {
        super(context);
        this.groupExpression = expression;
    }

    @Override
    public String toString() {
        return "DeriveStatsTask for groupExpression " + groupExpression;
    }

    @Override
    public void execute() {
        if (groupExpression.isStatsDerived() || groupExpression.isUnused()) {
            return;
        }

        for (int i = groupExpression.arity() - 1; i >= 0; --i) {
            GroupExpression childExpression = groupExpression.getInputs().get(i).getFirstLogicalExpression();
            Preconditions.checkState(childExpression.isStatsDerived());
            Preconditions.checkNotNull(groupExpression.getInputs().get(i).getStatistics());
        }

        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                context.getOptimizerContext().getColumnRefFactory(), context.getOptimizerContext());
        statisticsCalculator.estimatorStats();

        Statistics currentStatistics = groupExpression.getGroup().getStatistics();
        // choose best statistics
        if (currentStatistics == null ||
                (expressionContext.getStatistics().getOutputRowCount() < currentStatistics.getOutputRowCount())) {
            groupExpression.getGroup().setStatistics(expressionContext.getStatistics());
        }
        if (currentStatistics != null && !currentStatistics.equals(expressionContext.getStatistics())) {
            if (groupExpression.getOp() instanceof LogicalOlapScanOperator) {
                LogicalOlapScanOperator scan = groupExpression.getOp().cast();
                if (scan.getTable().isMaterializedView()) {
                    PhysicalOlapScanOperator physicalOlapScan = new PhysicalOlapScanOperator(
                            scan.getTable(),
                            scan.getColRefToColumnMetaMap(),
                            scan.getDistributionSpec(),
                            scan.getLimit(),
                            scan.getPredicate(),
                            scan.getSelectedIndexId(),
                            scan.getSelectedPartitionId(),
                            scan.getSelectedTabletId(),
                            scan.getPrunedPartitionPredicates(),
                            scan.getProjection());
                    GroupExpression newGroupExpression = new GroupExpression(physicalOlapScan, groupExpression.getInputs());
                    groupExpression.getGroup().setGroupExpressionStatistics(newGroupExpression,
                            expressionContext.getStatistics());
                }
            }
        }

        groupExpression.setStatsDerived();
    }
}