// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.task;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerTraceInfo;
import com.starrocks.sql.optimizer.OptimizerTraceUtil;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.Binder;
import com.starrocks.sql.optimizer.rule.Rule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * ApplyRuleTask firstly applies a rule, then
 * <p>
 * If the rule is transformation rule and exploreOnly is true:
 * We need to explore (apply logical rules)
 * <p>
 * If the rule is transformation rule and exploreOnly is false:
 * We need to optimize (apply logical & physical rules)
 * <p>
 * If the rule is implementation rule:
 * We directly enforce and cost the physical expression
 */

public class ApplyRuleTask extends OptimizerTask {
    private static final Logger LOG = LogManager.getLogger(ApplyRuleTask.class);
    private final GroupExpression groupExpression;
    private final Rule rule;

    ApplyRuleTask(TaskContext context, GroupExpression groupExpression, Rule rule) {
        super(context);
        this.groupExpression = groupExpression;
        this.rule = rule;
    }

    @Override
    public String toString() {
        return "ApplyRuleTask for groupExpression " + groupExpression +
                "\n rule " + rule;
    }

    private void deriveLogicalProperty(OptExpression root) {
        if (!(root.getOp() instanceof LogicalOperator)) {
            return;
        }
        for (OptExpression child : root.getInputs()) {
            deriveLogicalProperty(child);
        }

        ExpressionContext context = new ExpressionContext(root);
        context.deriveLogicalProperty();
        root.setLogicalProperty(context.getRootProperty());
    }

    @Override
    public void execute() {
        if (groupExpression.hasRuleExplored(rule) ||
                groupExpression.isUnused()) {
            return;
        }
        SessionVariable sessionVariable = context.getOptimizerContext().getSessionVariable();
        // Apply rule and get all new OptExpressions
        Pattern pattern = rule.getPattern();
        Binder binder = new Binder(pattern, groupExpression);
        OptExpression extractExpr = binder.next();
        List<OptExpression> newExpressions = Lists.newArrayList();
        int extractNum = 0;
        while (extractExpr != null) {
            if (!rule.check(extractExpr, context.getOptimizerContext())) {
                extractExpr = binder.next();
                continue;
            }
            List<OptExpression> targetExpressions = rule.transform(extractExpr, context.getOptimizerContext());

            int newExpressionNum = 0;
            for (OptExpression expression : targetExpressions) {
                LOG.info("newExpressionNum:{}, rule:{}", newExpressionNum++, rule.type());
                deriveLogicalProperty(expression);
                if (!(expression.getOp() instanceof LogicalOperator)) {
                    continue;
                }
                Map<ColumnRefOperator, ScalarOperator> projectionMap1 = getProjectionMap(expression,
                        context.getOptimizerContext().getColumnRefFactory());
                LOG.info("new expression projection:{}", projectionMap1);
            }

            newExpressions.addAll(targetExpressions);

            OptimizerTraceInfo traceInfo = context.getOptimizerContext().getTraceInfo();
            OptimizerTraceUtil.logApplyRule(sessionVariable, traceInfo, rule, extractExpr, targetExpressions);

            Map<ColumnRefOperator, ScalarOperator> projectionMap = getProjectionMap(extractExpr,
                    context.getOptimizerContext().getColumnRefFactory());
            LOG.info("extractNum:{}, rule:{}, projection:%{}", extractNum++, rule.type(), projectionMap);
            extractExpr = binder.next();
        }

        for (OptExpression expression : newExpressions) {
            // Insert new OptExpression to memo
            Pair<Boolean, GroupExpression> result = context.getOptimizerContext().getMemo().
                    copyIn(groupExpression.getGroup(), expression);

            // The group has been merged
            if (groupExpression.hasEmptyRootGroup()) {
                return;
            }

            // TODO(kks) optimize this
            GroupExpression newGroupExpression = result.second;
            if (newGroupExpression.getOp().isLogical()) {
                // For logic newGroupExpression, optimize it
                pushTask(new OptimizeExpressionTask(context, newGroupExpression));
                pushTask(new DeriveStatsTask(context, newGroupExpression));
            } else {
                // For physical newGroupExpression, enforce and cost it,
                // Optimize its inputs if needed
                pushTask(new EnforceAndCostTask(context, newGroupExpression));
            }
        }

        groupExpression.setRuleExplored(rule);
    }

    Map<ColumnRefOperator, ScalarOperator> getProjectionMap(OptExpression expression, ColumnRefFactory columnRefFactory) {
        if (expression.getOp().getProjection() != null) {
            return expression.getOp().getProjection().getColumnRefMap();
        } else {
            Map<ColumnRefOperator, ScalarOperator> projectionMap = Maps.newHashMap();
            ColumnRefSet columnRefSet = expression.getOutputColumns();
            for (int columnId : columnRefSet.getColumnIds()) {
                ColumnRefOperator columnRef = columnRefFactory.getColumnRef(columnId);
                projectionMap.put(columnRef, columnRef);
            }
            return projectionMap;
        }
    }
}
