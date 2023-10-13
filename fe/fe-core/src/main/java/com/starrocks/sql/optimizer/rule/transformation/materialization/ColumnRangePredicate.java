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

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

// all ranges about one column ref
// eg: a > 10 and a < 100 => a -> (10, 100)
// (a > 10 and a < 100) or (a > 1000 and a <= 10000) => a -> (10, 100) or (1000, 10000]
public class ColumnRangePredicate extends RangePredicate {
    private ScalarOperator expression;
    private ColumnRefOperator columnRef;
    // the relation between each Range in RangeSet is 'or'
    private TreeRangeSet<ConstantOperator> columnRanges;

    private TreeRangeSet<ConstantOperator> canonicalColumnRanges;

    public ColumnRangePredicate(ScalarOperator expression, TreeRangeSet<ConstantOperator> columnRanges) {
        this.expression = expression;
        List<ColumnRefOperator> columns = Utils.collect(expression, ColumnRefOperator.class);
        Preconditions.checkState(columns.size() == 1);
        this.columnRef = columns.get(0);
        this.columnRanges = columnRanges;
        List<Range<ConstantOperator>> canonicalRanges = new ArrayList<>();
        if (ConstantOperatorDiscreteDomain.isSupportedType(this.expression.getType())) {
            for (Range range : this.columnRanges.asRanges()) {
                Range canonicalRange = range.canonical(new ConstantOperatorDiscreteDomain());
                canonicalRanges.add(canonicalRange);
            }
            this.canonicalColumnRanges = TreeRangeSet.create(canonicalRanges);
        } else {
            this.canonicalColumnRanges = columnRanges;
        }
    }

    /*
    public ColumnRangePredicate(ColumnRefOperator columnRef, TreeRangeSet<ConstantOperator> columnRanges) {
        this.expression = columnRef;
        this.columnRef = columnRef;
        this.columnRanges = columnRanges;
        List<Range<ConstantOperator>> canonicalRanges = new ArrayList<>();
        if (ConstantOperatorDiscreteDomain.isSupportedType(columnRef.getType())) {
            for (Range range : this.columnRanges.asRanges()) {
                Range canonicalRange = range.canonical(new ConstantOperatorDiscreteDomain());
                canonicalRanges.add(canonicalRange);
            }
            this.canonicalColumnRanges = TreeRangeSet.create(canonicalRanges);
        } else {
            this.canonicalColumnRanges = columnRanges;
        }
    }

     */

    public ScalarOperator getExpression() {
        return expression;
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
        return new ColumnRangePredicate(rangePredicate.getExpression(), TreeRangeSet.create(ranges));
    }

    public static ColumnRangePredicate orRange(
            ColumnRangePredicate rangePredicate, ColumnRangePredicate otherRangePredicate) {
        TreeRangeSet<ConstantOperator> result = TreeRangeSet.create();
        result.addAll(rangePredicate.columnRanges);
        result.addAll(otherRangePredicate.columnRanges);
        return new ColumnRangePredicate(rangePredicate.getExpression(), result);
    }

    public boolean isUnbounded() {
        return columnRanges.asRanges().stream().allMatch(range -> !range.hasUpperBound() && !range.hasLowerBound());
    }

    public List<ColumnRangePredicate> getEquivalentRangePredicates() {
        if (isCastDate() || isStr2Date()) {
            return getEquivalentRangePredicateForDate();
        }
        return Lists.newArrayList();
    }

    boolean isCastDate() {
        if (!(expression instanceof CastOperator)) {
            return false;
        }
        CastOperator castOperator = expression.cast();
        return castOperator.getChild(0).isColumnRef()
                && castOperator.getChild(0).getType().isStringType()
                && castOperator.getType().isDate();
    }

    boolean isStr2Date() {
        if (!(expression instanceof CallOperator)) {
            return false;
        }
        CallOperator callOperator = expression.cast();
        // check whether is str2date(columnref, '%Y-%m-%d')
        return callOperator.getFnName().equalsIgnoreCase(FunctionSet.STR2DATE)
                && callOperator.getChild(0).isColumnRef() &&
                ((ConstantOperator) callOperator.getChild(1)).getChar().equals("%Y-%m-%d");
    }

    // may return date with different format, so this function returns List<ColumnRangePredicate>
    public List<ColumnRangePredicate> getEquivalentRangePredicateForDate() {
        TreeRangeSet<ConstantOperator> stringRangeSet = TreeRangeSet.create();
        // convert constant date to constant string
        for (Range<ConstantOperator> range : canonicalColumnRanges.asRanges()) {
            Range<ConstantOperator> stringRange = convertRange(range);
            stringRangeSet.add(stringRange);
        }
        ColumnRangePredicate rangePredicate = new ColumnRangePredicate(columnRef, stringRangeSet);
        return Lists.newArrayList(rangePredicate);
    }

    private Range<ConstantOperator> convertRange(Range<ConstantOperator> from) {
        if (from.hasLowerBound() && from.hasUpperBound()) {
            return Range.range(ConstantOperator.createChar(from.lowerEndpoint().getDate().toString(), Type.VARCHAR),
                    from.lowerBoundType(),
                    ConstantOperator.createChar(from.upperEndpoint().getDate().toString(), Type.VARCHAR),
                    from.upperBoundType());
        } else if (from.hasUpperBound()) {
            return Range.upTo(ConstantOperator.createChar(from.upperEndpoint().getDate().toString(), Type.VARCHAR),
                    from.upperBoundType());
        } else if (from.hasLowerBound()) {
            Range.downTo(ConstantOperator.createChar(from.upperEndpoint().getDate().toString(), Type.VARCHAR),
                    from.lowerBoundType());
        }
        return Range.all();
    }

    @Override
    public boolean enclose(RangePredicate other) {
        if (!(other instanceof ColumnRangePredicate)) {
            return false;
        }
        ColumnRangePredicate columnRangePredicate = other.cast();
        boolean isEnclosed = canonicalColumnRanges.enclosesAll(columnRangePredicate.canonicalColumnRanges);
        if (isEnclosed) {
            return true;
        }
        // is equivalences enclosed
        List<ColumnRangePredicate> equivalences = getEquivalentRangePredicates();
        List<ColumnRangePredicate> otherEquivalences = columnRangePredicate.getEquivalentRangePredicates();
        if (equivalences.isEmpty() || otherEquivalences.isEmpty()) {
            return false;
        }
        return equivalences.stream().anyMatch(e -> otherEquivalences.stream().anyMatch(o -> e.enclose(o)));
    }

    @Override
    public ScalarOperator toScalarOperator() {
        List<ScalarOperator> orOperators = Lists.newArrayList();
        for (Range<ConstantOperator> range : columnRanges.asRanges()) {
            List<ScalarOperator> andOperators = Lists.newArrayList();
            if (range.hasLowerBound() && range.hasUpperBound()) {
                if (range.lowerBoundType() == BoundType.CLOSED
                        && range.upperBoundType() == BoundType.CLOSED
                        && range.upperEndpoint().equals(range.lowerEndpoint())) {
                    orOperators.add(BinaryPredicateOperator.eq(expression, range.lowerEndpoint()));
                    continue;
                } else if (range.lowerBoundType() == BoundType.CLOSED
                        && range.upperBoundType() == BoundType.OPEN
                        && range.lowerEndpoint().successor().isPresent()
                        && range.upperEndpoint().equals(range.lowerEndpoint().successor().get())) {
                    orOperators.add(BinaryPredicateOperator.eq(expression, range.lowerEndpoint()));
                    continue;
                }
            }
            if (range.hasLowerBound()) {
                if (range.lowerBoundType() == BoundType.CLOSED) {
                    andOperators.add(BinaryPredicateOperator.ge(expression, range.lowerEndpoint()));
                } else {
                    andOperators.add(BinaryPredicateOperator.gt(expression, range.lowerEndpoint()));
                }
            }

            if (range.hasUpperBound()) {
                if (range.upperBoundType() == BoundType.CLOSED) {
                    andOperators.add(BinaryPredicateOperator.le(expression, range.upperEndpoint()));
                } else {
                    andOperators.add(BinaryPredicateOperator.lt(expression, range.upperEndpoint()));
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
            if (!expression.equals(otherColumnRangePredicate.expression)) {
                return null;
            }
            /*
            if (!columnRef.equals(otherColumnRangePredicate.getColumnRef())) {
                return null;
            }

             */
            if (columnRanges.equals(otherColumnRangePredicate.columnRanges)
                    || canonicalColumnRanges.equals(otherColumnRangePredicate.canonicalColumnRanges)) {
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
            for (RangePredicate rangePredicate : orRangePredicate.getChildPredicates()) {
                ScalarOperator simplied = simplify(rangePredicate);
                if (simplied != null) {
                    return toScalarOperator();
                }
            }
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnRangePredicate that = (ColumnRangePredicate) o;
        return Objects.equals(columnRef, that.columnRef) && Objects.equals(columnRanges, that.columnRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnRef, columnRanges);
    }
}
