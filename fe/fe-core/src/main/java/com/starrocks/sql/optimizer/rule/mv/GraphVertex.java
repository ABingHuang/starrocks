package com.starrocks.sql.optimizer.rule.mv;

import java.util.ArrayList;
import java.util.List;

// info for groph vertex
// including: outEdges and inEdges
public class GraphVertex<E> {
    private final List<E> outEdges = new ArrayList<>();
    private final List<E> inEdges = new ArrayList<>();

    public List<E> getOutEdges() {
        return outEdges;
    }

    public List<E> getInEdges() {
        return inEdges;
    }
}
