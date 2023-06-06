package com.starrocks.sql.optimizer.rule.mv;

public interface EdgeFactory<V, E> {
    E createEdge(V v0, V v1);
}
