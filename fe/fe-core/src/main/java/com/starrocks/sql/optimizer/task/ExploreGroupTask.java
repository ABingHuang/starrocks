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


package com.starrocks.sql.optimizer.task;

import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;

/*
 * Explore group logical transform
 */
public class ExploreGroupTask extends OptimizerTask {
    private final Group group;

    ExploreGroupTask(TaskContext context, Group group) {
        super(context);
        this.group = group;
    }

    @Override
    public void execute() {
        if (group.isExplored()) {
            // 这里一个group只会explore一次
            // 因为explore一次之后，group就会调用所有的rule生成所有可能的logical expression了，
            // 没有必要多次调用，
            // ***这里有一个核心的前提条件***，比较隐晦，就是整个优化的执行是自底向上的执行，
            // 一定是下一层的plan都已经优化好了之后，再执行上层的OptimizeExpression的操作
            // 后面就是根据搜索的context（parent required property set之类的进行物理计划的生成和enforce工作）
            return;
        }

        for (GroupExpression logical : group.getLogicalExpressions()) {
            // 只会explore expression
            // 注意：isExplore设置为true
            pushTask(new OptimizeExpressionTask(context, logical, true));
        }

        // memorization，只生成一次
        group.setExplored();
    }

    @Override
    public String toString() {
        return "ExploreGroupTask for group " + group.getId();
    }
}
