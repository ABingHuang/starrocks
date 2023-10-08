// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Map;

// a virtual scan operator used by mv rewrite, which packages the complex logical plan, make
// it satisfy SPJG pattern, and make the mv rewrite rules more general.
public class LogicalBoxOperator extends LogicalScanOperator {
    private final OptExpression equalLogicalPlan;

    private LogicalBoxOperator(OptExpression equalLogicalPlan) {
        super(OperatorType.LOGICAL_BOX_SCAN);
        this.equalLogicalPlan = equalLogicalPlan;
    }

    public static LogicalBoxOperator create(OptExpression equalLogicalPlan, OptimizerContext optimizerContext) {
        Preconditions.checkState(equalLogicalPlan.getOp() instanceof LogicalOperator);
        LogicalOperator op = equalLogicalPlan.getOp().cast();
        ExpressionContext context = new ExpressionContext(equalLogicalPlan);
        context.deriveLogicalProperty();
        ColumnRefSet refSet = op.getOutputColumns(context);
        List<ColumnRefOperator> outputColumns = refSet.getColumnRefOperators(optimizerContext.getColumnRefFactory());
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnMetaToColRefMap = Maps.newHashMap();
        for (ColumnRefOperator columnRef : outputColumns) {
            Column column = new Column(columnRef.getName(), columnRef.getType(), columnRef.isNullable());
            colRefToColumnMetaMap.put(columnRef, column);
            columnMetaToColRefMap.put(column, columnRef);
        }
        LogicalBoxOperator.Builder builder = new Builder();
        builder.setColRefToColumnMetaMap(colRefToColumnMetaMap);
        builder.setColumnMetaToColRefMap(columnMetaToColRefMap);
        builder.setEqualLogicalPlan(equalLogicalPlan);
        // should we care about table?
        return builder.build();
    }

    public OptExpression getEqualLogicalPlan() {
        return equalLogicalPlan;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalBox(this, context);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalBoxOperator, LogicalBoxOperator.Builder> {
        private OptExpression equalLogicalPlan;
        @Override
        protected LogicalBoxOperator newInstance() {
            return new LogicalBoxOperator(equalLogicalPlan);
        }

        @Override
        public Builder withOperator(LogicalBoxOperator boxOperator) {
            super.withOperator(boxOperator);
            this.equalLogicalPlan = boxOperator.equalLogicalPlan;
            return this;
        }

        public Builder setEqualLogicalPlan(OptExpression equalLogicalPlan) {
            this.equalLogicalPlan = equalLogicalPlan;
            return this;
        }
    }
}

