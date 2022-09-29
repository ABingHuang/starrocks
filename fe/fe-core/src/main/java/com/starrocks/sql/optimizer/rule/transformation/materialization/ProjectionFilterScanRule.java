// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

/*
 *
 * Here is the rule for pattern Projection - Filter - Scan
 * Keep single table rewrite in rule-base phase to reduce the search space
 *
 */
public class ProjectionFilterScanRule extends BaseMaterializedViewRewriteRule {
    public ProjectionFilterScanRule() {
        super(RuleType.TF_MV_PROJECT_FILTER_SCAN_RULE, Pattern.create(OperatorType.LOGICAL_PROJECT)
                .addChildren(Pattern.create(OperatorType.LOGICAL_FILTER, OperatorType.PATTERN_SCAN)));
    }
}
