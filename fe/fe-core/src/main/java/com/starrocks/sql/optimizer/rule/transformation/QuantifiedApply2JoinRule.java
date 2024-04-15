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


package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.SubqueryUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.MultiInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

// 这个类和QuantifiedApply2OuterJoinRule的区别
// 先执行QuantifiedApply2JoinRule，QuantifiedApply2OuterJoinRule
// 两者的check的条件只相差一下isUseSemiAnti
// 当isUseSemiAnti为true的时候，会使用QuantifiedApply2JoinRule，将Apply转成left semi join或者null aware left anti join
//      那什么情况下isUseSemiAnti为true呢？只有当谓词是纯粹的InPredicate或者ExistPredicate的时候，isUseSemiAnti才为true。否则，
//      在SqlToScalarOperatorTranslator中，Context clone的时候，会把isUseSemiAnti设置为false
// 否则，使用QuantifiedApply2OuterJoinRule，将类转成cte + left outer join/cross join
public class QuantifiedApply2JoinRule extends TransformationRule {
    public QuantifiedApply2JoinRule() {
        super(RuleType.TF_QUANTIFIED_APPLY_TO_JOIN,
                Pattern.create(OperatorType.LOGICAL_APPLY, OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();
        return apply.isUseSemiAnti() && apply.isQuantified()
                && !SubqueryUtils.containsCorrelationSubquery(input);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();
        boolean isNotIn = false;
        ScalarOperator simplifiedPredicate = null;
        if (apply.getSubqueryOperator() instanceof MultiInPredicateOperator) {
            MultiInPredicateOperator multiIn = (MultiInPredicateOperator) apply.getSubqueryOperator();
            isNotIn = multiIn.isNotIn();
            List<ScalarOperator> conjuncts = Lists.newArrayList();
            ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
            for (int i = 0; i < multiIn.getTupleSize(); ++i) {
                ScalarOperator left = multiIn.getChild(i);
                ScalarOperator right = multiIn.getChild(multiIn.getTupleSize() + i);
                ScalarOperator normalizedConjunct =
                        rewriter.rewrite(new BinaryPredicateOperator(BinaryType.EQ,
                                left, right), ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
                conjuncts.add(normalizedConjunct);
            }
            simplifiedPredicate = Utils.compoundAnd(conjuncts);
        } else {
            // IN/NOT IN
            InPredicateOperator ipo = (InPredicateOperator) apply.getSubqueryOperator();
            BinaryPredicateOperator bpo =
                    new BinaryPredicateOperator(BinaryType.EQ, ipo.getChildren());
            isNotIn = ipo.isNotIn();

            ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
            simplifiedPredicate =
                    rewriter.rewrite(bpo, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);

        }

        // IN to SEMI-JOIN
        // NOT IN to ANTI-JOIN or NULL_AWARE_LEFT_ANTI_JOIN
        OptExpression joinExpression;
        if (isNotIn) {
            //@TODO: if will can filter null, use left-anti-join
            List<ScalarOperator> correlatedConjuncts = Utils.extractConjuncts(apply.getCorrelationConjuncts());
            correlatedConjuncts.forEach(conjunct -> conjunct.setCorrelated(true));
            joinExpression = new OptExpression(new LogicalJoinOperator(JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN,
                    Utils.compoundAnd(simplifiedPredicate,
                            Utils.compoundAnd(Utils.compoundAnd(correlatedConjuncts), apply.getPredicate()))));
        } else {
            joinExpression = new OptExpression(new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN,
                    Utils.compoundAnd(simplifiedPredicate,
                            Utils.compoundAnd(apply.getCorrelationConjuncts(), apply.getPredicate()))));
        }

        joinExpression.getInputs().addAll(input.getInputs());

        Map<ColumnRefOperator, ScalarOperator> outputColumns = input.getOutputColumns().getStream().map(
                id -> context.getColumnRefFactory().getColumnRef(id)
        ).collect(Collectors.toMap(Function.identity(), Function.identity()));
        return Lists.newArrayList(
                OptExpression.create(new LogicalProjectOperator(outputColumns), Lists.newArrayList(joinExpression)));
    }
}
