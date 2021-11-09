// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/column_predicate.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef STARROCKS_BE_SRC_OLAP_COLUMN_PREDICATE_H
#define STARROCKS_BE_SRC_OLAP_COLUMN_PREDICATE_H

#include <roaring/roaring.hh>

#include "storage/column_block.h"
#include "storage/rowset/segment_v2/bitmap_index_reader.h"
#include "storage/selection_vector.h"

using namespace starrocks::segment_v2;

namespace starrocks {

class VectorizedRowBatch;
class Schema;
class ObjectPool;

class ColumnPredicate {
public:
    explicit ColumnPredicate(uint32_t column_id) : _column_id(column_id) {}

    virtual ~ColumnPredicate() = default;

    //evaluate predicate on VectorizedRowBatch
    virtual void evaluate(VectorizedRowBatch* batch) const = 0;

    // evaluate predicate on ColumnBlock
    virtual void evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const = 0;

    //evaluate predicate on Bitmap
    virtual Status evaluate(const Schema& schema, const std::vector<BitmapIndexIterator*>& iterators, uint32_t num_rows,
                            Roaring* roaring) const = 0;

    uint32_t column_id() const { return _column_id; }

    virtual Status convert_to(const ColumnPredicate** outpout, FieldType from_type, FieldType to_type,
                              ObjectPool* obj_pool) const = 0;

protected:
    uint32_t _column_id;
};

} //namespace starrocks

#endif //STARROCKS_BE_SRC_OLAP_COLUMN_PREDICATE_H
