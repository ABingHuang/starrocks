package com.starrocks.sql.optimizer.rule.mv;

import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

public class JoinEdge<V> extends DefaultEdge<V> {
    private JoinOperator joinOperator;
    private ScalarOperator onPredicate;

    public JoinEdge(V source, V target) {
        super(source, target);
    }

    public JoinOperator getJoinOperator() {
        return joinOperator;
    }

    public void setJoinOperator(JoinOperator joinOperator) {
        this.joinOperator = joinOperator;
    }

    public ScalarOperator getOnPredicate() {
        return onPredicate;
    }

    public void setOnPredicate(ScalarOperator onPredicate) {
        this.onPredicate = onPredicate;
    }
}
