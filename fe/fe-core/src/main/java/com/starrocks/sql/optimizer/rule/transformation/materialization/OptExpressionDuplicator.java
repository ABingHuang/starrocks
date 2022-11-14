// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.List;
import java.util.Map;

// used in SPJG mv union rewrite
// OptExpressionDuplicator will duplicate the OptExpression tree,
// and rewrite all ColumnRefOperator from bottom to up,
// and put the ColumnRefOperator mapping in columnMapping
// Now only logical Scan/Filter/Projection/Join/Aggregation operator are supported
public class OptExpressionDuplicator {
    private final ColumnRefFactory columnRefFactory;
    // old ColumnRefOperator -> new ColumnRefOperator
    private final Map<ColumnRefOperator, ScalarOperator> columnMapping;
    private ReplaceColumnRefRewriter rewriter;
    public OptExpressionDuplicator(ColumnRefFactory columnRefFactory) {
        this.columnRefFactory = columnRefFactory;
        this.columnMapping = Maps.newHashMap();
        this.rewriter = new ReplaceColumnRefRewriter(columnMapping);
    }
    public OptExpression duplicate(OptExpression source) {
        OptExpressionDuplicatorVisitor visitor = new OptExpressionDuplicatorVisitor();
        return source.getOp().accept(visitor, source, null);
    }

    public List<ColumnRefOperator> getMappedColumns(List<ColumnRefOperator> originColumns) {
        List<ColumnRefOperator> newColumnRefs = Lists.newArrayList();
        for (ColumnRefOperator columnRef : originColumns) {
            newColumnRefs.add((ColumnRefOperator) columnMapping.get(columnRef));
        }
        return newColumnRefs;
    }

    class OptExpressionDuplicatorVisitor extends OptExpressionVisitor<OptExpression, Void> {
        @Override
        public OptExpression visitLogicalTableScan(OptExpression optExpression, Void context) {
            Operator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(optExpression.getOp());
            Map<ColumnRefOperator, Column> columnRefOperatorColumnMap =
                    ((LogicalScanOperator) optExpression.getOp()).getColRefToColumnMetaMap();
            ImmutableMap.Builder<ColumnRefOperator, Column> columnRefColumnMapBuilder = new ImmutableMap.Builder<>();
            for (Map.Entry<ColumnRefOperator, Column> entry : columnRefOperatorColumnMap.entrySet()) {
                ColumnRefOperator key = entry.getKey();
                ColumnRefOperator newColumnRef = columnRefFactory.create(key, key.getType(), key.isNullable());
                columnRefColumnMapBuilder.put(newColumnRef, entry.getValue());
                columnMapping.put(entry.getKey(), newColumnRef);
                columnRefFactory.updateColumnRef(key, newColumnRef);
            }
            ImmutableMap<ColumnRefOperator, Column> newColumnRefColumnMap = columnRefColumnMapBuilder.build();
            LogicalScanOperator.Builder scanBuilder = (LogicalScanOperator.Builder) opBuilder;
            scanBuilder.setColRefToColumnMetaMap(newColumnRefColumnMap);

            Map<Column, ColumnRefOperator> columnMetaToColRefMap =
                    ((LogicalScanOperator) optExpression.getOp()).getColumnMetaToColRefMap();
            ImmutableMap.Builder<Column, ColumnRefOperator> columnMetaToColRefMapBuilder = new ImmutableMap.Builder<>();
            for (Map.Entry<Column, ColumnRefOperator> entry : columnMetaToColRefMap.entrySet()) {
                ColumnRefOperator key = entry.getValue();
                if (columnMapping.containsKey(key)) {
                    columnMetaToColRefMapBuilder.put(entry.getKey(), (ColumnRefOperator) columnMapping.get(key));
                } else {
                    ColumnRefOperator newColumnRef = columnRefFactory.create(key, key.getType(), key.isNullable());
                    columnMetaToColRefMapBuilder.put(entry.getKey(), newColumnRef);
                    columnMapping.put(key, newColumnRef);
                    columnRefFactory.updateColumnRef(key, newColumnRef);
                }
            }
            ImmutableMap<Column, ColumnRefOperator> newColumnMetaToColRefMap = columnMetaToColRefMapBuilder.build();
            scanBuilder.setColumnMetaToColRefMap(newColumnMetaToColRefMap);

            processCommon(opBuilder);

            return OptExpression.create(opBuilder.build());
        }

        @Override
        public OptExpression visitLogicalProject(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = Lists.newArrayList();
            for (OptExpression child : optExpression.getInputs()) {
                inputs.add(child.getOp().accept(this, child, null));
            }
            Operator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(optExpression.getOp());
            processCommon(opBuilder);

            LogicalProjectOperator projectOperator = (LogicalProjectOperator) optExpression.getOp();
            Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = processProjection(projectOperator.getColumnRefMap());
            LogicalProjectOperator.Builder projectBuilder = (LogicalProjectOperator.Builder) opBuilder;
            projectBuilder.setColumnRefMap(newColumnRefMap);
            return OptExpression.create(projectBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalAggregate(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = Lists.newArrayList();
            for (OptExpression child : optExpression.getInputs()) {
                inputs.add(child.getOp().accept(this, child, null));
            }
            Operator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(optExpression.getOp());

            LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) optExpression.getOp();
            List<ColumnRefOperator> newGroupKeys = Lists.newArrayList();
            for (ColumnRefOperator groupKey : aggregationOperator.getGroupingKeys()) {
                ColumnRefOperator mapped = (ColumnRefOperator) columnMapping.computeIfAbsent(groupKey, k -> {
                    ColumnRefOperator newColumnRef = columnRefFactory.create(k, k.getType(), k.isNullable());
                    return newColumnRef;
                });
                newGroupKeys.add(mapped);
            }
            LogicalAggregationOperator.Builder aggregationBuilder  = (LogicalAggregationOperator.Builder) opBuilder;
            aggregationBuilder.setGroupingKeys(newGroupKeys);
            Map<ColumnRefOperator, CallOperator> newAggregates = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregationOperator.getAggregations().entrySet()) {
                ColumnRefOperator mapped = (ColumnRefOperator) columnMapping.computeIfAbsent(entry.getKey(), k -> {
                    ColumnRefOperator newColumnRef = columnRefFactory.create(k, k.getType(), k.isNullable());
                    return newColumnRef;
                });
                ScalarOperator newValue = rewriter.rewrite(entry.getValue());
                Preconditions.checkState(newValue instanceof CallOperator);
                newAggregates.put(mapped, (CallOperator) newValue);
            }
            aggregationBuilder.setAggregations(newAggregates);

            List<ColumnRefOperator> newPartitionColumns = Lists.newArrayList();
            List<ColumnRefOperator> partitionColumns = aggregationOperator.getPartitionByColumns();
            if (partitionColumns != null) {
                for (ColumnRefOperator columnRef : partitionColumns) {
                    ColumnRefOperator mapped = (ColumnRefOperator) columnMapping.computeIfAbsent(columnRef, k -> {
                        ColumnRefOperator newColumnRef = columnRefFactory.create(k, k.getType(), k.isNullable());
                        return newColumnRef;
                    });
                    newPartitionColumns.add(mapped);
                }
            }
            aggregationBuilder.setPartitionByColumns(newPartitionColumns);
            processCommon(opBuilder);

            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalJoin(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = Lists.newArrayList();
            for (OptExpression child : optExpression.getInputs()) {
                inputs.add(child.getOp().accept(this, child, null));
            }
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();
            Operator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(optExpression.getOp());

            processCommon(opBuilder);

            ScalarOperator onpredicate = joinOperator.getOnPredicate();
            if (onpredicate != null) {
                ScalarOperator newOnPredicate = rewriter.rewrite(onpredicate);
                LogicalJoinOperator.Builder joinBuilder = (LogicalJoinOperator.Builder) opBuilder;
                joinBuilder.setOnPredicate(newOnPredicate);
            }
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visitLogicalFilter(OptExpression optExpression, Void context) {
            List<OptExpression> inputs = Lists.newArrayList();
            for (OptExpression child : optExpression.getInputs()) {
                inputs.add(child.getOp().accept(this, child, null));
            }
            Operator.Builder opBuilder = OperatorBuilderFactory.build(optExpression.getOp());
            opBuilder.withOperator(optExpression.getOp());
            LogicalFilterOperator filterOperator = (LogicalFilterOperator) optExpression.getOp();
            if (filterOperator.getPredicate() != null) {
                ScalarOperator newPredicate = rewriter.rewrite(filterOperator.getPredicate());
                LogicalFilterOperator.Builder filterBuilder = (LogicalFilterOperator.Builder) opBuilder;
                filterBuilder.setPredicate(newPredicate);
            }
            return OptExpression.create(opBuilder.build(), inputs);
        }

        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {
            return null;
        }

        private void processCommon(Operator.Builder opBuilder) {
            ScalarOperator predicate = opBuilder.getPredicate();
            if (predicate != null) {
                ScalarOperator newPredicate = rewriter.rewrite(predicate);
                opBuilder.setPredicate(newPredicate);
            }
            Projection projection = opBuilder.getProjection();
            if (projection != null) {
                Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = processProjection(projection.getColumnRefMap());
                Projection newProjection = new Projection(newColumnRefMap);
                opBuilder.setProjection(newProjection);
            }
        }

        private Map<ColumnRefOperator, ScalarOperator> processProjection(Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
            Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : columnRefMap.entrySet()) {
                ColumnRefOperator column = entry.getKey();
                if (!columnMapping.containsKey(column)) {
                    ColumnRefOperator newColumnRef = columnRefFactory.create(column, column.getType(), column.isNullable());
                    columnMapping.put(column, newColumnRef);
                }
                ScalarOperator newValue = rewriter.rewrite(entry.getValue());
                newColumnRefMap.put((ColumnRefOperator) columnMapping.get(column), newValue);
            }
            return newColumnRefMap;
        }
    }
}
