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

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Comparator;
import java.util.List;

/**
 * If isExplore is true:
 * OptimizeExpressionTask explores a GroupExpression by constructing all logical
 * transformations and applying those rules, and to drive group's statistics.
 * <p>
 * If isExplore is false:
 * OptimizeExpressionTask optimizes a GroupExpression by constructing all logical
 * and physical transformations and applying those rules, and to drive group's statistics.
 * <p>
 * The rules are sorted by promise and applied in that order
 * so a physical transformation rule is applied before a logical transformation rule.
 */
public class OptimizeExpressionTask extends OptimizerTask {
    private final GroupExpression groupExpression;
    private final boolean isExplore;

    OptimizeExpressionTask(TaskContext context, GroupExpression expression) {
        super(context);
        this.groupExpression = expression;
        this.isExplore = false;
    }

    OptimizeExpressionTask(TaskContext context, GroupExpression expression, boolean isExplore) {
        super(context);
        this.groupExpression = expression;
        this.isExplore = isExplore;
    }

    private List<Rule> getValidRules() {
        List<Rule> validRules = Lists.newArrayListWithCapacity(RuleType.NUM_RULES.id());
        List<Rule> logicalRules = context.getOptimizerContext().getRuleSet().getTransformRules();
        filterInValidRules(groupExpression, logicalRules, validRules);

        if (!isExplore) {
            // 在explore的时候，只需要调用transformation rule生成logical expression就可以，所以不需要implement rules
            List<Rule> physicalRules = context.getOptimizerContext().getRuleSet().getImplementRules();
            filterInValidRules(groupExpression, physicalRules, validRules);
        }

        // 根据promise进行rule的排序，transformation为1，implement rule为2
        // 所以会先做transformation，然后再进行implement
        // 因为得现有逻辑计划，才能生成物理计划
        // 这样子调用一次的task就可以把逻辑计划和物理计划都生成了
        validRules.sort(Comparator.comparingInt(Rule::promise));
        return validRules;
    }

    @Override
    public String toString() {
        return "OptimizeExpressionTask" + (this.isExplore ? "[explore]" : "")
                + " for groupExpression " + groupExpression;
    }

    @Override
    public void execute() {
        List<Rule> rules = getValidRules();

        // 这里有一个核心点就是
        // 这里是先递归的ExploreGroupTask之后，再进行ApplyRuleTask
        // 也就是意味着在优化当前的group expresssion之前，
        // 所有的subtree的所有可能的逻辑子计划都已经生成好了
        // 这样子在当前的group expresssion的binding过程中，就是在探索所有可能的情况了
        // 这个本质就是执行是自底向上的执行！！！
        for (Rule rule : rules) {
            pushTask(new ApplyRuleTask(context, groupExpression, rule, isExplore));
        }

        pushTask(new DeriveStatsTask(context, groupExpression));
        for (int i = groupExpression.arity() - 1; i >= 0; i--) {
            pushTask(new ExploreGroupTask(context, groupExpression.getInputs().get(i)));
        }
    }
}
