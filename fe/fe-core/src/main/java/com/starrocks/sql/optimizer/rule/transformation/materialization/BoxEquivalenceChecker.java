package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalBoxOperator;
import org.apache.commons.collections4.CollectionUtils;

import java.util.stream.Stream;

public class BoxEquivalenceChecker extends OptExpressionVisitor<Boolean, OptExpression> {
    // check boxed tree from top to down
    public boolean check(LogicalBoxOperator one, LogicalBoxOperator another) {
        return one.accept(this, one.getEqualLogicalPlan(), another.getEqualLogicalPlan());
    }

    @Override
    public Boolean visitLogicalAggregate(OptExpression optExpression, OptExpression context) {
        Preconditions.checkState(context.getOp() instanceof LogicalAggregationOperator);
        LogicalAggregationOperator queryAgg = optExpression.getOp().cast();
        LogicalAggregationOperator mvAgg = optExpression.getOp().cast();
        boolean ret = queryAgg.getType() == mvAgg.getType();
        if (!ret) {
            return false;
        }
        if (queryAgg.getGroupingKeys().size() != mvAgg.getGroupingKeys().size()) {
            return false;
        }
        CollectionUtils.isEqualCollection(queryAgg, mvAgg, );
    }
}
