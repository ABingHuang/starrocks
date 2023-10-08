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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalBoxOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;

import java.util.List;

import static com.starrocks.sql.optimizer.rule.RuleType.TF_MV_BOXING_RULE;

// now it is for boxing of mv
// TODO: is it suitable for query?
// unsupported operator: order by(glabal)，
public class MVRewriteBoxingRule extends TransformationRule {
    private MVRewriteBoxingRule() {
        super(TF_MV_BOXING_RULE, Pattern.create(OperatorType.PATTERN_LEAF));
    }

    public static MVRewriteBoxingRule getInstance() {
        return INSTANCE;
    }

    private static final MVRewriteBoxingRule INSTANCE = new MVRewriteBoxingRule();

    public boolean check(final OptExpression input, OptimizerContext context) {
        return !MvUtils.isLogicalSPJG(input) && !MvUtils.isLogicalSPJ(input);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        return Lists.newArrayList(new MvBoxingShuttle().visit(input, context));
    }

    private static class MvBoxingShuttle extends OptExpressionVisitor<OptExpression, OptimizerContext> {
        @Override
        public OptExpression visitLogicalTableScan(OptExpression tableScan, OptimizerContext context) {
            return tableScan;
        }

        @Override
        public OptExpression visitLogicalAggregate(OptExpression agg, OptimizerContext context) {
            return MvUtils.isLogicalSPJG(agg) ? agg : createBoxExpression(agg, context);
        }

        @Override
        public OptExpression visitLogicalJoin(OptExpression join, OptimizerContext context) {
            List<OptExpression> children = Lists.newArrayList();
            for (OptExpression child : join.getInputs()) {
                children.add(visit(child, context));
            }

            boolean isLeftSPJ = MvUtils.isLogicalSPJ(children.get(0));
            boolean isRightSPJ = MvUtils.isLogicalSPJ(children.get(1));
            if (isLeftSPJ && isRightSPJ) {
                return join;
            }
            OptExpression left = isLeftSPJ ? children.get(0) : createBoxExpression(children.get(0), context);
            OptExpression right = isRightSPJ ? children.get(1) : createBoxExpression(children.get(1), context);
            return OptExpression.create(join.getOp(), left, right);
        }

        @Override
        public OptExpression visitLogicalUnion(OptExpression union, OptimizerContext context) {
            return createBoxExpression(union, context);
        }

        @Override
        public OptExpression visitLogicalExcept(OptExpression except, OptimizerContext context) {
            return createBoxExpression(except, context);
        }

        @Override
        public OptExpression visitLogicalIntersect(OptExpression intersect, OptimizerContext context) {
            return createBoxExpression(intersect, context);
        }

        @Override
        public OptExpression visitLogicalWindow(OptExpression window, OptimizerContext context) {
            return createBoxExpression(window, context);
        }

        private OptExpression createBoxExpression(OptExpression optExpression, OptimizerContext context) {
            return OptExpression.create(LogicalBoxOperator.create(optExpression, context));
        }
    }
}
