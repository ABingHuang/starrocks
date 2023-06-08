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
