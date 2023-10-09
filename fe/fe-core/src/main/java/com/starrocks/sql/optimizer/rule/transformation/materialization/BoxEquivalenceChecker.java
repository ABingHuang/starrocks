package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalBoxOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

public class BoxEquivalenceChecker extends OptExpressionVisitor<Boolean, OptExpression> {
    private final static BoxEquivalenceChecker INSTANCE = new BoxEquivalenceChecker();

    public static BoxEquivalenceChecker getInstance() {
        return INSTANCE;
    }

    // check boxed tree from top to down
    // here we should care the logical equivalence, not equal
    // we pay attention to the result produced by logical operator, not the physical distribution
    public boolean check(LogicalBoxOperator one, LogicalBoxOperator another) {
        return one.accept(this, one.getEqualLogicalPlan(), another.getEqualLogicalPlan());
    }

    @Override
    public Boolean visitLogicalAggregate(OptExpression optExpression, OptExpression context) {
        Preconditions.checkState(context.getOp() instanceof LogicalAggregationOperator);
        LogicalAggregationOperator queryAgg = optExpression.getOp().cast();
        LogicalAggregationOperator mvAgg = context.getOp().cast();
        return processOperator(queryAgg, mvAgg)
                && processAgg(queryAgg, mvAgg)
                && processChild(optExpression, context);
    }

    private boolean processAgg(LogicalAggregationOperator queryAgg, LogicalAggregationOperator mvAgg) {
        boolean ret = queryAgg.getType() == mvAgg.getType()
                && queryAgg.getGroupingKeys().size() == mvAgg.getGroupingKeys().size();
        if (!ret) {
            return false;
        }
        for (int i = 0; i < queryAgg.getGroupingKeys().size(); i++) {
            ColumnRefOperator queryKey = queryAgg.getGroupingKeys().get(i);
            ColumnRefOperator mvKey = mvAgg.getGroupingKeys().get(i);
            if (!queryKey.equivalent(mvKey)) {
                return false;
            }
            if (!queryAgg.getAggregations().get(queryKey).equivalent(mvAgg.getAggregations().get(mvKey))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean visitLogicalJoin(OptExpression optExpression, OptExpression context) {
        Preconditions.checkState(context.getOp() instanceof LogicalJoinOperator);
        LogicalJoinOperator queryJoin = optExpression.getOp().cast();
        LogicalJoinOperator mvJoin = context.getOp().cast();
        boolean ret = queryJoin.getJoinType() == mvJoin.getJoinType()
                && queryJoin.getOnPredicate().equivalent(mvJoin.getOnPredicate())
                && processOperator(queryJoin, mvJoin);
        if (!ret) {
            return false;
        }
        return processChild(optExpression, context);
    }

    @Override
    public Boolean visitLogicalTableScan(OptExpression optExpression, OptExpression context) {
        Preconditions.checkState(context.getOp() instanceof LogicalScanOperator);
        LogicalScanOperator queryScan = optExpression.getOp().cast();
        LogicalScanOperator mvScan = context.getOp().cast();
        if (queryScan.getOpType() != mvScan.getOpType()) {
            return false;
        }
        if (queryScan.getOpType() == OperatorType.LOGICAL_BOX_SCAN) {
            LogicalBoxOperator queryBox = queryScan.cast();
            LogicalBoxOperator mvBox = mvScan.cast();
            // recursive judge whether boxes are equivalent
            return queryBox.accept(this, queryBox.getEqualLogicalPlan(), mvBox.getEqualLogicalPlan());
        } else if (queryScan.getOpType() == OperatorType.LOGICAL_OLAP_SCAN) {
            boolean ret = processOperator(queryScan, mvScan) && processChild(optExpression, context);
            if (!ret) {
                return false;
            }
            LogicalOlapScanOperator queryOlapScan = queryScan.cast();
            LogicalOlapScanOperator mvOlapScan = mvScan.cast();
            List<ScalarOperator> partitionPredicates1 = queryOlapScan.getPrunedPartitionPredicates();
            List<ScalarOperator> partitionPredicates2 = mvOlapScan.getPrunedPartitionPredicates();
            if (partitionPredicates1 == null && partitionPredicates2 == null) {
                return true;
            } else if (partitionPredicates1 == null || partitionPredicates2 == null) {
                return false;
            }
            return IntStream.range(0, queryOlapScan.getPrunedPartitionPredicates().size())
                    .allMatch(i -> partitionPredicates1.get(i).equivalent(partitionPredicates2.get(i)));
        } else {
            // unsupported now
            return false;
        }
    }

    private boolean processOperator(LogicalOperator query, LogicalOperator mv) {
        return query.getOpType() == mv.getOpType()
                && query.getLimit() == mv.getLimit()
                && query.getPredicate() == null ? mv.getPredicate() == null : query.getPredicate().equivalent(mv.getPredicate())
                && processProjection(query.getProjection(), mv.getProjection());
    }

    private boolean processProjection(Projection queryProjection, Projection mvProjection) {
        return checkMapEquivalence(queryProjection.getColumnRefMap(), mvProjection.getColumnRefMap())
                && checkMapEquivalence(queryProjection.getCommonSubOperatorMap(), mvProjection.getCommonSubOperatorMap());
    }

    private boolean checkMapEquivalence(
            Map<ColumnRefOperator, ScalarOperator> queryMap,
            Map<ColumnRefOperator, ScalarOperator> mvMap) {
        if (queryMap == null && mvMap == null) {
            return true;
        } else if (queryMap == null || mvMap == null) {
            return false;
        }
        if (queryMap.size() != mvMap.size()) {
            return false;
        }
        if (queryMap.isEmpty()) {
            return true;
        }
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : queryMap.entrySet()) {
            Optional<ColumnRefOperator> mvKey = mvMap.keySet().stream()
                    .filter(key -> key.equivalent(entry.getKey())).findAny();
            if (!mvKey.isPresent()) {
                return false;
            }
            if (!entry.getValue().equivalent(mvMap.get(mvKey.get()))) {
                return false;
            }
        }
        return true;
    }

    private boolean processChild(OptExpression optExpression, OptExpression context) {
        for (int i = 0; i < optExpression.getInputs().size(); i++) {
            boolean childRet = optExpression.inputAt(i).getOp().accept(this, optExpression.inputAt(i), context.inputAt(i));
            if (!childRet) {
                return false;
            }
        }
        return true;
    }
}
