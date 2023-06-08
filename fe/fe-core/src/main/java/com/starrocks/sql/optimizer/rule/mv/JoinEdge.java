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
