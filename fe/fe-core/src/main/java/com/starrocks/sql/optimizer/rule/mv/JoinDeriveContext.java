package com.starrocks.sql.optimizer.rule.mv;

import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;

public class JoinDeriveContext {
    private JoinOperator queryJoinType;
    private JoinOperator mvJoinType;
    private List<ColumnRefOperator> joinColumns;

    public JoinDeriveContext(
            JoinOperator queryJoinType,
            JoinOperator mvJoinType,
            List<ColumnRefOperator> joinColumns) {
        this.queryJoinType = queryJoinType;
        this.mvJoinType = mvJoinType;
        this.joinColumns = joinColumns;
    }

    public JoinOperator getQueryJoinType() {
        return queryJoinType;
    }

    public JoinOperator getMvJoinType() {
        return mvJoinType;
    }

    public List<ColumnRefOperator> getJoinColumns() {
        return joinColumns;
    }
}
