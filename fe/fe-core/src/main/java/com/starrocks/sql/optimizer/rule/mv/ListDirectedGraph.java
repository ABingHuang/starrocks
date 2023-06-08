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

package com.starrocks.sql.optimizer.rule.mv;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

// a directed graph whose vertexes and edges can be customed
public class ListDirectedGraph<V, E extends DefaultEdge> implements IDirectedGraph<V, E> {
    final Set<E> edges = new LinkedHashSet<>();
    final Map<V, GraphVertex<E>> vertexMap = new LinkedHashMap<>();
    final EdgeFactory<V, E> edgeFactory;

    public ListDirectedGraph(EdgeFactory<V, E> edgeFactory) {
        this.edgeFactory = edgeFactory;
    }

    public static <V, E extends DefaultEdge> ListDirectedGraph<V, E> create(
            EdgeFactory<V, E> edgeFactory) {
        return new ListDirectedGraph<>(edgeFactory);
    }

    @Override
    public boolean addVertex(V v) {
        if (vertexMap.containsKey(v)) {
            return false;
        } else {
            vertexMap.put(v, new GraphVertex<>());
            return true;
        }
    }

    @Override
    public void removeVertex(V v) {
        vertexMap.remove(v);
    }

    @Override
    public E addEdge(V source, V target) {
        final GraphVertex<E> srcVertex = getVertex(source);
        final GraphVertex<E> targetVertex = getVertex(target);
        final E edge = edgeFactory.createEdge(source, target);
        if (edges.add(edge)) {
            srcVertex.getOutEdges().add(edge);
            targetVertex.getInEdges().add(edge);
            return edge;
        } else {
            return null;
        }
    }

    @Override
    public E getEdge(V source, V target) {
        final GraphVertex<E> srcVertex = getVertex(source);
        for (E edge : srcVertex.getOutEdges()) {
            if (edge.getTarget().equals(target)) {
                return edge;
            }
        }
        return null;
    }

    @Override
    public Set<? extends V> vertexSet() {
        return vertexMap.keySet();
    }

    @Override
    public Set<? extends E> edgeSet() {
        return edges;
    }

    @Override
    public List<E> getOutwardEdges(V source) {
        return getVertex(source).getOutEdges();
    }

    @Override
    public List<E> getInwardEdges(V target) {
        return getVertex(target).getInEdges();
    }

    private final GraphVertex<E> getVertex(V vertex) {
        final GraphVertex<E> node = vertexMap.get(vertex);
        if (node == null) {
            throw new IllegalArgumentException("no vertex " + vertex);
        }
        return node;
    }
}
