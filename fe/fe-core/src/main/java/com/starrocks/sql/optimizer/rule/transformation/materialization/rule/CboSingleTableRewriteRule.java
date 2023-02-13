package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.Binder;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class CboSingleTableRewriteRule extends Rule {
    public CboSingleTableRewriteRule() {
        super(RuleType.TF_MV_CBO_SINGLE_TABLE_REWRITE_RULE, Pattern.create(OperatorType.PATTERN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        rewrite(input, context);
        return Lists.newArrayList();
    }

    private void rewrite(OptExpression input, OptimizerContext context) {
        List<Rule> rules = context.getRuleSet().getRewriteRulesByType(RuleSetType.SINGLE_TABLE_MV_REWRITE);

        List<OptExpression> newExpressions = Lists.newArrayList();
        for (Rule rule : rules) {
            Pattern pattern = rule.getPattern();
            Binder binder = new Binder(pattern, input.getGroupExpression());
            OptExpression extractExpr = binder.next();

            while (extractExpr != null) {
                if (!rule.check(extractExpr, context)) {
                    extractExpr = binder.next();
                    continue;
                }
                List<OptExpression> targetExpressions = rule.transform(extractExpr, context);
                if (targetExpressions != null && !targetExpressions.isEmpty()) {
                    newExpressions.addAll(targetExpressions);
                }

                extractExpr = binder.next();
            }
        }

        for (OptExpression expression : newExpressions) {
            // Insert new OptExpression to memo
            context.getMemo().copyIn(input.getGroupExpression().getGroup(), expression);
        }

        // prune cte column depend on prune right child first
        for (int i = input.getInputs().size() - 1; i >= 0; i--) {
            rewrite(input.getInputs().get(i), context);
        }
    }

    /*
    private void deriveLogicalProperty(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            deriveLogicalProperty(child);
        }

        if (root.getLogicalProperty() == null) {
            ExpressionContext context = new ExpressionContext(root);
            context.deriveLogicalProperty();
            root.setLogicalProperty(context.getRootProperty());
        }
    }

     */
}
