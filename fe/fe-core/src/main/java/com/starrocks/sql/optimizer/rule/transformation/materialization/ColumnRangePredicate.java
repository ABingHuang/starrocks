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


package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.List;

public class ColumnRangePredicate extends RangePredicate{
    private ColumnRefOperator columnRef;
    private TreeRangeSet<ConstantOperator> columnRanges;

    public ColumnRangePredicate(ColumnRefOperator columnRef, TreeRangeSet<ConstantOperator> columnRanges) {
        this.columnRef = columnRef;
        this.columnRanges = columnRanges;
    }

    public ColumnRefOperator getColumnRef() {
        return columnRef;
    }

    public static ColumnRangePredicate andRange(
            ColumnRangePredicate rangePredicate, ColumnRangePredicate otherRangePredicate) {
        List<Range<ConstantOperator>> ranges = new ArrayList<>();
        for (Range<ConstantOperator> range : rangePredicate.columnRanges.asRanges()) {
            if (otherRangePredicate.columnRanges.intersects(range)) {
                for (Range<ConstantOperator> otherRange : otherRangePredicate.columnRanges.asRanges()) {
                    if (range.isConnected(otherRange)) {
                        Range<ConstantOperator> intersection = range.intersection(otherRange);
                        if (!intersection.isEmpty()) {
                            ranges.add(intersection);
                        }
                    }
                }
            }
        }
        return new ColumnRangePredicate(rangePredicate.columnRef, TreeRangeSet.create(ranges));
    }

    public static ColumnRangePredicate orRange(
            ColumnRangePredicate rangePredicate, ColumnRangePredicate otherRangePredicate) {
        TreeRangeSet<ConstantOperator> result = TreeRangeSet.create();
        result.addAll(rangePredicate.columnRanges);
        result.addAll(otherRangePredicate.columnRanges);
        return new ColumnRangePredicate(rangePredicate.getColumnRef(), result);
    }

    public boolean isUnbounded() {
        return columnRanges.asRanges().stream().allMatch(range -> !range.hasUpperBound() && !range.hasLowerBound());
        // && !columnRanges.asRanges().stream().allMatch(Range::hasLowerBound);
    }

    @Override
    public boolean enclose(RangePredicate other) {
        if (!(other instanceof ColumnRangePredicate)) {
            return false;
        }
        ColumnRangePredicate columnRangePredicate = other.cast();
        return columnRanges.enclosesAll(columnRangePredicate.columnRanges);
    }

    @Override
    public ScalarOperator toScalarOperator() {
        List<ScalarOperator> orOperators = Lists.newArrayList();
        for (Range<ConstantOperator> range : columnRanges.asRanges()) {
            List<ScalarOperator> andOperators = Lists.newArrayList();
            if (range.hasLowerBound() && range.hasUpperBound() && range.upperEndpoint().equals(range.lowerEndpoint())) {
                andOperators.add(BinaryPredicateOperator.eq(columnRef, range.upperEndpoint()));
            } else {
                if (range.hasLowerBound()) {
                    if (range.lowerBoundType() == BoundType.CLOSED) {
                        andOperators.add(BinaryPredicateOperator.ge(columnRef, range.lowerEndpoint()));
                    } else {
                        andOperators.add(BinaryPredicateOperator.gt(columnRef, range.lowerEndpoint()));
                    }
                }

                if (range.hasUpperBound()) {
                    if (range.upperBoundType() == BoundType.CLOSED) {
                        andOperators.add(BinaryPredicateOperator.le(columnRef, range.upperEndpoint()));
                    } else {
                        andOperators.add(BinaryPredicateOperator.lt(columnRef, range.upperEndpoint()));
                    }
                }
            }
            orOperators.add(Utils.compoundAnd(andOperators));
        }
        return Utils.compoundOr(orOperators);
    }

    @Override
    public ScalarOperator simplify(RangePredicate other) {
        if (this.equals(other)) {
            return ConstantOperator.TRUE;
        }
        if (other instanceof ColumnRangePredicate) {
            ColumnRangePredicate otherColumnRangePredicate = (ColumnRangePredicate) other;
            if (!columnRef.equals(otherColumnRangePredicate.getColumnRef())) {
                return null;
            }
            if (columnRanges.equals(otherColumnRangePredicate.columnRanges)) {
                return ConstantOperator.TRUE;
            } else {
                if (other.enclose(this)) {
                    return toScalarOperator();
                }
                return null;
            }
        } else if (other instanceof AndRangePredicate) {
            return null;
        } else {
            OrRangePredicate orRangePredicate = (OrRangePredicate) other;
            for (RangePredicate rangePredicate : orRangePredicate.getRangePredicates()) {
                ScalarOperator simplied = simplify(rangePredicate);
                if (simplied != null) {
                    return simplied;
                }
            }
            return null;
        }
    }
}
