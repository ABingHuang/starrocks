// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

/*
 *
 * Here is the rule for pattern Aggregate-Filter-Scan
 *
 */
public class AggregateFilterScanRule extends AggregateRewriteBaseRule {
    private static AggregateFilterScanRule INSTANCE = new AggregateFilterScanRule();

    public AggregateFilterScanRule() {
        super(RuleType.TF_MV_AGGREGATE_FILTER_SCAN_RULE, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.LOGICAL_FILTER))
                .addChildren(Pattern.create(OperatorType.PATTERN_SCAN)));
    }

    public static AggregateFilterScanRule getInstance() {
        return INSTANCE;
    }
}