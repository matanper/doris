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

// Aggregate states that dedup strings keep them in an arena that is freed only with the state.
// These tests feed many duplicates and check that the arena holds each distinct value once.

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "core/arena.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/string_buffer.hpp"
#include "exprs/aggregate/aggregate_function_collect.h"
#include "exprs/aggregate/aggregate_function_distinct.h"

namespace doris {
namespace {

constexpr size_t DISTINCT_VALUES = 10;
constexpr size_t VALUE_SIZE = 200;
constexpr size_t ROWS = 20000;

std::string value_at(size_t i) {
    std::string value(VALUE_SIZE, static_cast<char>('a' + (i % DISTINCT_VALUES)));
    value += std::to_string(i % DISTINCT_VALUES);
    return value;
}

std::vector<std::string> distinct_values() {
    std::vector<std::string> values;
    for (size_t i = 0; i < DISTINCT_VALUES; ++i) {
        values.push_back(value_at(i));
    }
    return values;
}

size_t distinct_bytes() {
    size_t bytes = 0;
    for (const auto& value : distinct_values()) {
        bytes += value.size();
    }
    return bytes;
}

// ROWS rows cycling through DISTINCT_VALUES values.
ColumnString::MutablePtr repeated_strings(size_t rows = ROWS) {
    auto column = ColumnString::create();
    for (size_t i = 0; i < rows; ++i) {
        const auto value = value_at(i);
        column->insert_data(value.data(), value.size());
    }
    return column;
}

std::vector<std::string> sorted(std::vector<std::string> values) {
    std::sort(values.begin(), values.end());
    return values;
}

std::vector<std::string> column_values(const IColumn& column) {
    std::vector<std::string> values;
    for (size_t i = 0; i < column.size(); ++i) {
        values.push_back(column.get_data_at(i).to_string());
    }
    return values;
}

using CollectSetString = AggregateFunctionCollectSetData<TYPE_STRING, false>;
using CollectSetStringLimit = AggregateFunctionCollectSetData<TYPE_STRING, true>;

template <typename Data>
std::vector<std::string> collect_set_result(const Data& data) {
    auto to = ColumnString::create();
    data.insert_result_into(*to);
    return sorted(column_values(*to));
}

TEST(AggArenaDedupCopyTest, CollectSetAddCopiesEachDistinctValueOnce) {
    auto column = repeated_strings();
    Arena arena;
    CollectSetString data({});
    for (size_t row = 0; row < column->size(); ++row) {
        data.add(*column, row, arena);
    }
    EXPECT_EQ(arena.used_size(), distinct_bytes());
    EXPECT_EQ(collect_set_result(data), sorted(distinct_values()));
}

TEST(AggArenaDedupCopyTest, CollectSetWithLimitAddCopiesEachDistinctValueOnce) {
    using Function = AggregateFunctionCollect<CollectSetStringLimit, true>;
    Function function({std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>()});

    auto strings = repeated_strings();
    auto limits = ColumnInt32::create(strings->size(), 100);
    const IColumn* columns[] = {strings.get(), limits.get()};

    Arena state_arena;
    auto* place = reinterpret_cast<AggregateDataPtr>(
            state_arena.aligned_alloc(function.size_of_data(), function.align_of_data()));
    function.create(place);

    Arena arena;
    for (size_t row = 0; row < strings->size(); ++row) {
        function.add(place, columns, row, arena);
    }
    EXPECT_EQ(arena.used_size(), distinct_bytes());
    EXPECT_EQ(collect_set_result(*reinterpret_cast<CollectSetStringLimit*>(place)),
              sorted(distinct_values()));
    function.destroy(place);
}

TEST(AggArenaDedupCopyTest, CollectSetLimitStopsCopyingAtTheCap) {
    auto column = repeated_strings();
    Arena arena;
    CollectSetStringLimit data({});
    data.max_size = 3;

    CollectSetStringLimit rhs({});
    Arena rhs_arena;
    rhs.max_size = 3;
    for (size_t row = 0; row < DISTINCT_VALUES; ++row) {
        rhs.add(*column, row, rhs_arena);
    }
    for (int i = 0; i < 100; ++i) {
        data.merge(rhs, arena);
    }
    EXPECT_EQ(data.size(), 3);
    EXPECT_EQ(arena.used_size(), 3 * (VALUE_SIZE + 1));
}

TEST(AggArenaDedupCopyTest, CollectSetMergeCopiesEachDistinctValueOnce) {
    auto column = repeated_strings(DISTINCT_VALUES);
    Arena rhs_arena;
    CollectSetString rhs({});
    for (size_t row = 0; row < column->size(); ++row) {
        rhs.add(*column, row, rhs_arena);
    }

    Arena arena;
    CollectSetString data({});
    for (int i = 0; i < 1000; ++i) {
        data.merge(rhs, arena);
    }
    EXPECT_EQ(arena.used_size(), distinct_bytes());
    EXPECT_EQ(collect_set_result(data), sorted(distinct_values()));
}

TEST(AggArenaDedupCopyTest, CollectSetDeserializeAndMergeCopiesEachDistinctValueOnce) {
    auto column = repeated_strings(DISTINCT_VALUES);
    Arena rhs_arena;
    CollectSetString rhs({});
    for (size_t row = 0; row < column->size(); ++row) {
        rhs.add(*column, row, rhs_arena);
    }
    auto serialized = ColumnString::create();
    {
        BufferWritable buf(*serialized);
        rhs.write(buf);
        buf.commit();
    }

    Arena arena;
    CollectSetString data({});
    for (int i = 0; i < 1000; ++i) {
        BufferReadable buf(serialized->get_data_at(0));
        CollectSetString tmp({});
        tmp.read(buf);
        data.merge(tmp, arena);
    }
    EXPECT_EQ(arena.used_size(), distinct_bytes());
    EXPECT_EQ(collect_set_result(data), sorted(distinct_values()));
}

template <bool stable>
std::vector<std::string> distinct_single_result(
        const AggregateFunctionDistinctSingleGenericData<stable>& data) {
    auto columns = data.get_arguments({std::make_shared<DataTypeString>()});
    return column_values(*columns[0]);
}

template <bool stable>
void check_distinct_single_add() {
    auto column = repeated_strings();
    const IColumn* columns[] = {column.get()};
    Arena arena;
    AggregateFunctionDistinctSingleGenericData<stable> data;
    for (size_t row = 0; row < column->size(); ++row) {
        data.add(columns, 1, row, arena);
    }
    EXPECT_EQ(arena.used_size(), distinct_bytes());
    if constexpr (stable) {
        // The stable form keeps first-seen order.
        EXPECT_EQ(distinct_single_result(data), distinct_values());
    } else {
        EXPECT_EQ(sorted(distinct_single_result(data)), sorted(distinct_values()));
    }
}

TEST(AggArenaDedupCopyTest, DistinctSingleAddCopiesEachDistinctValueOnce) {
    check_distinct_single_add<false>();
}

TEST(AggArenaDedupCopyTest, DistinctSingleStableAddCopiesEachDistinctValueOnce) {
    check_distinct_single_add<true>();
}

TEST(AggArenaDedupCopyTest, DistinctMergeCopiesEachDistinctValueOnce) {
    auto column = repeated_strings(DISTINCT_VALUES);
    const IColumn* columns[] = {column.get()};
    Arena rhs_arena;
    AggregateFunctionDistinctSingleGenericData<false> rhs;
    for (size_t row = 0; row < column->size(); ++row) {
        rhs.add(columns, 1, row, rhs_arena);
    }

    Arena arena;
    AggregateFunctionDistinctSingleGenericData<false> data;
    for (int i = 0; i < 1000; ++i) {
        data.merge(rhs, arena);
    }
    EXPECT_EQ(arena.used_size(), distinct_bytes());
    EXPECT_EQ(sorted(distinct_single_result(data)), sorted(distinct_values()));
}

template <bool stable>
void check_distinct_multiple_add() {
    auto first = repeated_strings();
    auto second = ColumnInt32::create();
    for (size_t row = 0; row < first->size(); ++row) {
        second->insert_value(static_cast<int32_t>(row % 2));
    }
    const IColumn* columns[] = {first.get(), second.get()};
    // DISTINCT_VALUES is even, so (value, row % 2) has DISTINCT_VALUES distinct pairs.
    const size_t distinct_rows = DISTINCT_VALUES;

    Arena reference_arena;
    AggregateFunctionDistinctMultipleGenericData<stable> reference;
    for (size_t row = 0; row < distinct_rows; ++row) {
        reference.add(columns, 2, row, reference_arena);
    }

    Arena arena;
    AggregateFunctionDistinctMultipleGenericData<stable> data;
    for (size_t row = 0; row < first->size(); ++row) {
        data.add(columns, 2, row, arena);
    }
    EXPECT_EQ(data.data.size(), distinct_rows);
    EXPECT_EQ(arena.used_size(), reference_arena.used_size());

    DataTypes types {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>()};
    auto result = data.get_arguments(types);
    auto expected = reference.get_arguments(types);
    ASSERT_EQ(result[0]->size(), expected[0]->size());
    if constexpr (stable) {
        EXPECT_EQ(column_values(*result[0]), column_values(*expected[0]));
        for (size_t i = 0; i < result[1]->size(); ++i) {
            EXPECT_EQ(result[1]->get_int(i), expected[1]->get_int(i));
        }
    } else {
        EXPECT_EQ(sorted(column_values(*result[0])), sorted(column_values(*expected[0])));
    }
}

TEST(AggArenaDedupCopyTest, DistinctMultipleAddKeepsEachDistinctKeyOnce) {
    check_distinct_multiple_add<false>();
}

TEST(AggArenaDedupCopyTest, DistinctMultipleStableAddKeepsEachDistinctKeyOnce) {
    check_distinct_multiple_add<true>();
}

// Keys are (int, string). When the int fits at the end of a chunk and the string does not,
// alloc_continue moves the int into a new chunk, leaving its 4 bytes behind in the old one.
// A duplicate key in that position must be rolled back from the new chunk.
template <bool stable>
void check_distinct_multiple_add_across_chunks() {
    constexpr size_t distinct_keys = 200;
    constexpr size_t repeats = 3;
    auto ints = ColumnInt32::create();
    auto strings = ColumnString::create();
    for (size_t key = 0; key < distinct_keys; ++key) {
        const std::string value = std::string(VALUE_SIZE, 'k') + std::to_string(1000 + key);
        for (size_t i = 0; i < repeats; ++i) {
            ints->insert_value(static_cast<int32_t>(key));
            strings->insert_data(value.data(), value.size());
        }
    }
    const IColumn* columns[] = {ints.get(), strings.get()};

    size_t int_size = 0;
    size_t key_size = 0;
    {
        Arena scratch;
        const char* begin = nullptr;
        int_size = ints->serialize_value_into_arena(0, scratch, begin).size;
        key_size = int_size + strings->serialize_value_into_arena(0, scratch, begin).size;
    }

    // Fixed 4096-byte chunks after the first, so there are many chunk boundaries.
    constexpr size_t first_chunk_size = 64;
    Arena arena(first_chunk_size, 1, 4096);
    AggregateFunctionDistinctMultipleGenericData<stable> data;
    size_t moved_keys = 0;
    size_t moved_duplicates = 0;
    for (size_t row = 0; row < ints->size(); ++row) {
        // The first chunk is created by the first allocation.
        const size_t remaining =
                row == 0 ? first_chunk_size : arena.remaining_space_in_current_chunk();
        if (remaining >= int_size && remaining < key_size) {
            ++moved_keys;
            if (row % repeats != 0) {
                ++moved_duplicates;
            }
        }
        data.add(columns, 2, row, arena);
    }
    ASSERT_GT(moved_duplicates, 0);
    EXPECT_EQ(data.data.size(), distinct_keys);
    // One copy per distinct key, plus the int prefix each moved key left in the previous chunk.
    EXPECT_EQ(arena.used_size(), distinct_keys * key_size + moved_keys * int_size);

    Arena reference_arena;
    AggregateFunctionDistinctMultipleGenericData<stable> reference;
    for (size_t row = 0; row < ints->size(); row += repeats) {
        reference.add(columns, 2, row, reference_arena);
    }
    DataTypes types {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()};
    auto result = data.get_arguments(types);
    auto expected = reference.get_arguments(types);
    ASSERT_EQ(result[1]->size(), distinct_keys);
    if constexpr (stable) {
        EXPECT_EQ(column_values(*result[1]), column_values(*expected[1]));
        for (size_t i = 0; i < distinct_keys; ++i) {
            EXPECT_EQ(result[0]->get_int(i), expected[0]->get_int(i));
        }
    } else {
        std::vector<std::string> got;
        std::vector<std::string> want;
        for (size_t i = 0; i < distinct_keys; ++i) {
            got.push_back(std::to_string(result[0]->get_int(i)) + "|" +
                          result[1]->get_data_at(i).to_string());
            want.push_back(std::to_string(expected[0]->get_int(i)) + "|" +
                           expected[1]->get_data_at(i).to_string());
        }
        EXPECT_EQ(sorted(got), sorted(want));
    }
}

TEST(AggArenaDedupCopyTest, DistinctMultipleAddRollsBackDuplicatesAcrossChunks) {
    check_distinct_multiple_add_across_chunks<false>();
}

TEST(AggArenaDedupCopyTest, DistinctMultipleStableAddRollsBackDuplicatesAcrossChunks) {
    check_distinct_multiple_add_across_chunks<true>();
}

} // namespace
} // namespace doris
