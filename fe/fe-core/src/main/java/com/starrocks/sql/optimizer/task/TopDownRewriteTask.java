// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.task;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.Group;
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
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * TopDownRewriteTask performs a top-down rewrite logic operator pass.
 * <p>
 * This class modify from CMU noisepage project TopDownRewrite class
 */
public abstract class TopDownRewriteTask extends OptimizerTask {
    private static final Logger LOG = LogManager.getLogger(TopDownRewriteTask.class);
    protected final Group group;
    protected final List<Rule> candidateRules;

    public TopDownRewriteTask(TaskContext context, Group group, List<Rule> ruleSet) {
        super(context);
        this.group = group;
        this.candidateRules = ruleSet;
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

    private Map<ColumnRefOperator, ScalarOperator> getProjectionMap(OptExpression expression,
                                                                    ColumnRefFactory columnRefFactory) {
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

    public void doExecute(boolean rewriteOnlyOnce) {
        List<Rule> validRules = Lists.newArrayListWithCapacity(RuleType.NUM_RULES.id());
        Preconditions.checkState(group.isValidInitState());

        GroupExpression curGroupExpression = group.getLogicalExpressions().get(0);
        filterInValidRules(curGroupExpression, candidateRules, validRules);
        SessionVariable sessionVariable = context.getOptimizerContext().getSessionVariable();

        for (Rule rule : validRules) {
            // Apply rule and get all new OptExpressions
            Pattern pattern = rule.getPattern();
            Binder binder = new Binder(pattern, curGroupExpression);
            OptExpression extractExpr = binder.next();
            List<OptExpression> newExpressions = Lists.newArrayList();
            while (extractExpr != null) {
                if (!rule.check(extractExpr, context.getOptimizerContext())) {
                    extractExpr = binder.next();
                    continue;
                }
                List<OptExpression> targetExpressions = rule.transform(extractExpr, context.getOptimizerContext());
                newExpressions.addAll(targetExpressions);

                OptimizerTraceInfo traceInfo = context.getOptimizerContext().getTraceInfo();
                OptimizerTraceUtil.logApplyRule(sessionVariable, traceInfo, rule, extractExpr, targetExpressions);

                Preconditions.checkState(newExpressions.size() <= 1,
                        "Rewrite rule should provide at most 1 expression");

                if (!newExpressions.isEmpty()) {

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

                    context.getOptimizerContext().getMemo().replaceRewriteExpression(
                            group, newExpressions.get(0));
                    // This group has been merged
                    if (group.getLogicalExpressions().isEmpty()) {
                        return;
                    }
                    OptExpression newExpression = new OptExpression(group.getFirstLogicalExpression());
                    LOG.info("newExpression:" + newExpression.explain());
                    if (group.getLogicalExpressions() != null) {
                        LOG.info("group logical property:" + group.getLogicalExpressions());
                    }

                    context.incrementRewriteNum();
                    if (rewriteOnlyOnce) {
                        curGroupExpression = group.getFirstLogicalExpression();
                        break;
                    } else {
                        pushTask(new TopDownRewriteIterativeTask(context, group, candidateRules));
                        return;
                    }
                }

                extractExpr = binder.next();
            }
        }

        for (Group childGroup : group.getFirstLogicalExpression().getInputs()) {
            if (rewriteOnlyOnce) {
                pushTask(new TopDownRewriteOnceTask(context, childGroup, candidateRules));
            } else {
                pushTask(new TopDownRewriteIterativeTask(context, childGroup, candidateRules));
            }
        }
    }
}
