// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public abstract class LogicalOperator extends Operator {

    protected LogicalOperator(OperatorType opType) {
        super(opType);
    }

    protected LogicalOperator(OperatorType operatorType, long limit, ScalarOperator predicate, Projection projection) {
        super(operatorType, limit, predicate, projection);
    }

    @Override
    public boolean isLogical() {
        return true;
    }

    public abstract ColumnRefSet getOutputColumns(ExpressionContext expressionContext);

    public Map<ColumnRefOperator, ScalarOperator> getLineage(
            ColumnRefFactory refFactory, ExpressionContext expressionContext) {
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = Maps.newHashMap();
        if (projection != null) {
            columnRefMap.putAll(projection.getColumnRefMap());
        } else {
            ColumnRefSet refSet = getOutputColumns(expressionContext);
            for (int columnId : refSet.getColumnIds()) {
                ColumnRefOperator columnRef = refFactory.getColumnRef(columnId);
                columnRefMap.put(columnRef, columnRef);
            }
        }
        return columnRefMap;
    }
}
