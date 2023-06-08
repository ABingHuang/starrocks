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
