// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

/*
 *
 * Here is the rule for pattern Aggregate-Filter-Join
 *
 */
public class AggregateFilterScanRule extends BaseMaterializedViewRewriteRule {
    private static AggregateFilterScanRule INSTANCE = new AggregateFilterScanRule();

    public AggregateFilterScanRule() {
        super(RuleType.TF_MV_AGGREGATE_FILTER_SCAN_RULE, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.LOGICAL_FILTER))
                .addChildren(Pattern.create(OperatorType.PATTERN_SCAN)), true);
    }

    public static AggregateFilterScanRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        Operator op = input.getOp();
        if (!(op instanceof LogicalAggregationOperator)) {
            return false;
        }
        LogicalAggregationOperator agg = (LogicalAggregationOperator) op;
        if (!agg.getType().equals(AggType.GLOBAL)) {
            return false;
        }
        return super.check(input, context);
    }
}