package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
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
// unsupported operator: order by(glabal)，
public class MVRewriteBoxingRule extends TransformationRule {
    private MVRewriteBoxingRule() {
        super(TF_MV_BOXING_RULE, Pattern.create(OperatorType.PATTERN_LEAF));
    }

    public static MVRewriteBoxingRule getInstance() {
        return INSTANCE;
    }

    private static final MVRewriteBoxingRule INSTANCE = new MVRewriteBoxingRule();

    public boolean check(final OptExpression input, OptimizerContext context) {
        return !MvUtils.isLogicalSPJG(input);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        return Lists.newArrayList(new MvBoxingShuttle().visit(input, null));
    }

    private static class MvBoxingShuttle extends OptExpressionVisitor<OptExpression, Void> {
        @Override
        public OptExpression visitLogicalTableScan(OptExpression tableScan, Void context) {
            return tableScan;
        }

        @Override
        public OptExpression visitLogicalAggregate(OptExpression agg, Void context) {
            return MvUtils.isLogicalSPJG(agg) ? agg : createBoxExpression(agg);
        }

        @Override
        public OptExpression visitLogicalJoin(OptExpression join, Void context) {
            List<OptExpression> children = Lists.newArrayList();
            for (OptExpression child : join.getInputs()) {
                children.add(visit(child, context));
            }

            boolean isLeftSPJ = MvUtils.isLogicalSPJ(children.get(0));
            boolean isRightSPJ = MvUtils.isLogicalSPJ(children.get(1));
            if (isLeftSPJ && isRightSPJ) {
                return join;
            }
            OptExpression left = isLeftSPJ ? children.get(0) : createBoxExpression(children.get(0));
            OptExpression right = isRightSPJ ? children.get(1) : createBoxExpression(children.get(1));
            return OptExpression.create(join.getOp(), left, right);
        }

        @Override
        public OptExpression visitLogicalUnion(OptExpression union, Void context) {
            return createBoxExpression(union);
        }

        @Override
        public OptExpression visitLogicalExcept(OptExpression except, Void context) {
            return createBoxExpression(except);
        }

        @Override
        public OptExpression visitLogicalIntersect(OptExpression intersect, Void context) {
            return createBoxExpression(intersect);
        }

        @Override
        public OptExpression visitLogicalWindow(OptExpression window, Void context) {
            return createBoxExpression(window);
        }

        private OptExpression createBoxExpression(OptExpression optExpression) {
            return OptExpression.create(LogicalBoxOperator.create(optExpression));
        }
    }
}
