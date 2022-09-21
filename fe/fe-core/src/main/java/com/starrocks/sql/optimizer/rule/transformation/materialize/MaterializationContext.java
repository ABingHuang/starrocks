// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialize;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Operator;

public class MaterializationContext {
    private MaterializedView mv;
    // scan materialized view operator
    private Operator scanMvOperator;
    // Logical OptExpression for query of materialized view
    private OptExpression mvExpression;

    public MaterializationContext(MaterializedView mv,
                                  Operator scanMvOperator,
                                  OptExpression mvExpression) {
        this.mv = mv;
        this.scanMvOperator = scanMvOperator;
        this.mvExpression = mvExpression;
    }

    public MaterializedView getMv() {
        return mv;
    }

    public Operator getScanMvOperator() {
        return scanMvOperator;
    }

    public OptExpression getMvExpression() {
        return mvExpression;
    }
}
