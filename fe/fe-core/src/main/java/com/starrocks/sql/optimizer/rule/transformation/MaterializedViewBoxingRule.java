package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalBoxOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;

import java.util.List;

import static com.starrocks.sql.optimizer.rule.RuleType.TF_MV_BOXING_RULE;

// now it is for boxing of mv
// TODO: is it suitable for query?
public class MaterializedViewBoxingRule extends TransformationRule {

    protected MaterializedViewBoxingRule() {
        super(TF_MV_BOXING_RULE, Pattern.create(OperatorType.PATTERN_LEAF));
    }

    public boolean check(final OptExpression input, OptimizerContext context) {
        return !MvUtils.isLogicalSPJG(input);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        return null;
    }

    private static class MvBoxingShuttle extends OptExpressionVisitor<OptExpression, OptExpression> {
        
        public OptExpression visitLogicalJoin(OptExpression join, OptExpression context) {
            boolean isLeftSPJ = MvUtils.isLogicalSPJ(join.inputAt(0));
            boolean isRightSPJ = MvUtils.isLogicalSPJ(join.inputAt(1));
            if (isLeftSPJ && isRightSPJ) {
                return join;
            }
            OptExpression left = isLeftSPJ ? join.inputAt(0) : createBoxExpression(join.inputAt(0));
            OptExpression right = isRightSPJ ? join.inputAt(1) : createBoxExpression(join.inputAt(1));
            return OptExpression.create(join.getOp(), left, right);
        }

        private OptExpression createBoxExpression(OptExpression optExpression) {
            return OptExpression.create(LogicalBoxOperator.create(optExpression));
        }
    }
}
