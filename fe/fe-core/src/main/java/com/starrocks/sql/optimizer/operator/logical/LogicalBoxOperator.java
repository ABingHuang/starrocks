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

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.OperatorType;

// a virtual scan operator used by mv rewrite, which packages the complex logical plan, make
// it satisfy SPJG pattern, and make the mv rewrite rules more general.
public class LogicalBoxOperator extends LogicalScanOperator {
    private final OptExpression equalLogicalPlan;

    private LogicalBoxOperator(OptExpression equalLogicalPlan) {
        super(OperatorType.LOGICAL_BOX_SCAN);
        this.equalLogicalPlan = equalLogicalPlan;
    }

    // TODO：构造column ref map
    public static LogicalBoxOperator create(OptExpression equalLogicalPlan) {
        return new LogicalBoxOperator(equalLogicalPlan);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalBoxOperator, LogicalBoxOperator.Builder> {
        private OptExpression equalLogicalPlan;
        @Override
        protected LogicalBoxOperator newInstance() {
            return new LogicalBoxOperator(equalLogicalPlan);
        }

        @Override
        public Builder withOperator(LogicalBoxOperator boxOperator) {
            super.withOperator(boxOperator);
            this.equalLogicalPlan = boxOperator.equalLogicalPlan;
            return this;
        }
    }
}

