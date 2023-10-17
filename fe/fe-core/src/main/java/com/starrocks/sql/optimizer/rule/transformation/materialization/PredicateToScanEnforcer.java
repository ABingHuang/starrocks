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


package com.starrocks.sql.optimizer.rule.transformation.materialization;

import autovalue.shaded.com.google.common.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class PredicateToScanEnforcer {
    private OptExpression optExpression;

    public PredicateToScanEnforcer(OptExpression optExpression) {
        this.optExpression = optExpression;
    }

    public OptExpression enforce(List<ScalarOperator> predicatesToEnforce) {
        PredicateToScanVisitor visitor = new PredicateToScanVisitor(predicatesToEnforce);
        OptExpression rootOptExpression = optExpression.getOp().accept(visitor, optExpression, null);
        return rootOptExpression;
    }

    private class PredicateToScanVisitor extends OptExpressionVisitor<OptExpression, Void> {
        private List<ScalarOperator> predicatesToEnforce;

        public PredicateToScanVisitor(List<ScalarOperator> predicatesToEnforce) {
            this.predicatesToEnforce = predicatesToEnforce;
        }

        @Override
        public OptExpression visitLogicalTableScan(OptExpression optExpression, Void context) {
            LogicalScanOperator scanOperator = optExpression.getOp().cast();
            LogicalScanOperator.Builder builder = OperatorBuilderFactory.build(scanOperator);
            builder.withOperator(scanOperator);
            Map<ColumnRefOperator, Column> columnRefOperatorColumnMap = Maps.newHashMap(scanOperator.getColRefToColumnMetaMap());
            Map<Column, ColumnRefOperator> columnMetaToColumnMap = scanOperator.getColumnMetaToColRefMap();
            Collection<ColumnRefOperator> allScanColumns = columnMetaToColumnMap.values();
            List<ScalarOperator> enforced = Lists.newArrayList();
            Projection originalProjection = scanOperator.getProjection();
            List<ColumnRefOperator> columnsEnforced = Lists.newArrayList();
            for (ScalarOperator predicate : predicatesToEnforce) {
                List<ColumnRefOperator> usedColumns = predicate.getColumnRefs();
                if (!allScanColumns.containsAll(usedColumns)) {
                    continue;
                }
                for (ColumnRefOperator columnRef : usedColumns) {
                    if (!columnRefOperatorColumnMap.containsKey(columnRef)) {
                        for (Map.Entry<Column, ColumnRefOperator> entry : columnMetaToColumnMap.entrySet()) {
                            if (entry.getValue().equals(columnRef)) {
                                columnRefOperatorColumnMap.put(columnRef, entry.getKey());
                                columnsEnforced.add(columnRef);
                            }
                        }
                    }
                }
                enforced.add(predicate);
            }
            if (enforced.isEmpty()) {
                return optExpression;
            }
            builder.setColRefToColumnMetaMap(columnRefOperatorColumnMap);
            enforced.add(scanOperator.getPredicate());
            builder.setPredicate(Utils.compoundAnd(enforced));
            if (originalProjection == null) {
                // we should add new Projection here
                Map<ColumnRefOperator, ScalarOperator> columnMap = Maps.newHashMap();
                for (ColumnRefOperator columnRef : columnRefOperatorColumnMap.keySet()) {
                    if (!columnsEnforced.contains(columnRef)) {
                        columnMap.put(columnRef, columnRef);
                    }
                }
                Projection projection = new Projection(columnMap);
                builder.setProjection(projection);
            }
            OptExpression newScan = new OptExpression(builder.build());
            newScan.deriveLogicalPropertyItself();
            predicatesToEnforce.removeAll(enforced);
            return newScan;
        }

        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = Lists.newArrayList();
            for (OptExpression child : optExpression.getInputs()) {
                inputs.add(child.getOp().accept(this, child, null));
            }

            LogicalOperator.Builder builder = OperatorBuilderFactory.build(optExpression.getOp());
            builder.withOperator(optExpression.getOp());
            OptExpression newOptExpression = OptExpression.create(builder.build(), inputs);
            newOptExpression.deriveLogicalPropertyItself();
            return newOptExpression;
        }
    }
}
