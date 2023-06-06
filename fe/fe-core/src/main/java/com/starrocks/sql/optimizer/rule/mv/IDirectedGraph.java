package com.starrocks.sql.optimizer.rule.mv;

import java.util.List;
import java.util.Set;

public interface IDirectedGraph<V, E> {
    boolean addVertex(final V v);

    void removeVertex(final V v);

    E addEdge(V vertex, V targetVertex);

    E getEdge(V source, V target);

    Set<? extends V> vertexSet();

    Set<? extends E> edgeSet();

    List<E> getOutwardEdges(V source);

    List<E> getInwardEdges(V vertex);
}
