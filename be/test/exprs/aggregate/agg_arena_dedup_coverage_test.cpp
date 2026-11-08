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

// collect_set (string) and the multi_distinct_<fn> generic states keep their keys in the
// aggregate arena, which is freed only with the states. These tests cover what that ownership
// requires: one arena copy per distinct key across add, repeated overlapping merges and
// deserialize-and-merge; keys that stay valid after the input column, the source state or the
// serialized buffer is gone; the stable form's first-insertion index; several arguments; NULL
// and empty strings; the max_size boundaries; all-unique input; and arena chunk growth.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/arena.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/string_buffer.hpp"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_collect.h"
#include "exprs/aggregate/aggregate_function_distinct.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {
namespace {

// Distinct values of different lengths, so payload sums are not a multiple of one size.
std::string value(size_t i) {
    return std::string(150 + (i * 37) % 101, static_cast<char>('a' + i % 26)) + "#" +
           std::to_string(i);
}

size_t payload(const std::vector<size_t>& ids) {
    size_t bytes = 0;
    for (size_t id : ids) {
        bytes += value(id).size();
    }
    return bytes;
}

std::vector<size_t> range(size_t begin, size_t end) {
    std::vector<size_t> ids;
    for (size_t i = begin; i < end; ++i) {
        ids.push_back(i);
    }
    return ids;
}

std::vector<std::string> values(const std::vector<size_t>& ids) {
    std::vector<std::string> out;
    for (size_t id : ids) {
        out.push_back(value(id));
    }
    return out;
}

std::vector<std::string> sorted(std::vector<std::string> v) {
    std::sort(v.begin(), v.end());
    return v;
}

// Rows of `ids`, each repeated `repeats` times in a round-robin order.
ColumnString::MutablePtr rows_of(const std::vector<size_t>& ids, size_t repeats) {
    auto column = ColumnString::create();
    for (size_t r = 0; r < repeats; ++r) {
        for (size_t id : ids) {
            const auto v = value(id);
            column->insert_data(v.data(), v.size());
        }
    }
    return column;
}

ColumnString::MutablePtr constant_strings(const std::string& s, size_t rows) {
    auto column = ColumnString::create();
    for (size_t i = 0; i < rows; ++i) {
        column->insert_data(s.data(), s.size());
    }
    return column;
}

AggregateFunctionPtr make_function(const std::string& name, const DataTypes& types) {
    auto function = AggregateFunctionSimpleFactory::instance().get(
            name, types, nullptr, false, BeExecVersionManager::get_newest_version());
    EXPECT_NE(function, nullptr) << name;
    return function;
}

// An aggregate state with its own storage; the keys live in the `arena` passed to add/merge.
class State {
public:
    explicit State(AggregateFunctionPtr function) : _function(std::move(function)) {
        place = reinterpret_cast<AggregateDataPtr>(_storage.aligned_alloc(
                _function->size_of_data(), _function->align_of_data()));
        _function->create(place);
    }
    ~State() { _function->destroy(place); }
    State(const State&) = delete;
    State& operator=(const State&) = delete;

    void add(const std::vector<const IColumn*>& columns, Arena& arena) {
        for (size_t row = 0; row < columns[0]->size(); ++row) {
            _function->add(place, const_cast<const IColumn**>(columns.data()), row, arena);
        }
    }

    void merge(const State& rhs, Arena& arena) { _function->merge(place, rhs.place, arena); }

    ColumnString::MutablePtr serialize() const {
        auto out = ColumnString::create();
        BufferWritable buf(*out);
        _function->serialize(place, buf);
        buf.commit();
        return out;
    }

    // Reads `serialized` into a temporary state and merges it, as the exchange receiver does.
    void deserialize_and_merge(const ColumnString& serialized, Arena& arena) {
        Arena rhs_storage;
        auto* rhs = reinterpret_cast<AggregateDataPtr>(rhs_storage.aligned_alloc(
                _function->size_of_data(), _function->align_of_data()));
        _function->create(rhs);
        BufferReadable buf(serialized.get_data_at(0));
        _function->deserialize_and_merge(place, rhs, buf, arena);
        _function->destroy(rhs);
    }

    AggregateDataPtr place = nullptr;

private:
    AggregateFunctionPtr _function;
    Arena _storage;
};

std::vector<std::string> collect_set_result(const IAggregateFunction& function,
                                            const State& state) {
    auto to = ColumnArray::create(ColumnNullable::create(ColumnString::create(),
                                                         ColumnUInt8::create()));
    function.insert_result_into(state.place, *to);
    const auto& nested = assert_cast<const ColumnNullable&>(to->get_data());
    std::vector<std::string> out;
    for (size_t i = 0; i < nested.size(); ++i) {
        out.push_back(nested.is_null_at(i) ? "<NULL>"
                                           : nested.get_nested_column().get_data_at(i).to_string());
    }
    return sorted(out);
}

std::vector<std::string> split(const std::string& s, const std::string& sep) {
    std::vector<std::string> out;
    if (s.empty()) {
        return out;
    }
    size_t begin = 0;
    while (true) {
        const size_t end = s.find(sep, begin);
        out.push_back(s.substr(begin, end == std::string::npos ? std::string::npos : end - begin));
        if (end == std::string::npos) {
            return out;
        }
        begin = end + sep.size();
    }
}

std::vector<std::string> group_concat_result(const IAggregateFunction& function,
                                             const State& state, const std::string& sep) {
    auto to = ColumnString::create();
    function.insert_result_into(state.place, *to);
    return sorted(split(to->get_data_at(0).to_string(), sep));
}

DataTypes string_type() {
    return {std::make_shared<DataTypeString>()};
}

// --- collect_set -------------------------------------------------------------------------

TEST(AggArenaDedupCoverageTest, CollectSetRepeatedOverlappingMergeAndDeserializeAndMerge) {
    auto function = make_function("collect_set", string_type());
    const auto left_ids = range(0, 7);
    const auto right_ids = range(3, 10);
    auto left_rows = rows_of(left_ids, 3);
    auto right_rows = rows_of(right_ids, 3);
    Arena left_arena;
    Arena right_arena;
    State left(function);
    State right(function);
    left.add({left_rows.get()}, left_arena);
    right.add({right_rows.get()}, right_arena);
    EXPECT_EQ(left_arena.used_size(), payload(left_ids));
    EXPECT_EQ(right_arena.used_size(), payload(right_ids));
    auto left_serialized = left.serialize();
    auto right_serialized = right.serialize();

    Arena arena;
    State dst(function);
    for (int i = 0; i < 200; ++i) {
        dst.merge(left, arena);
        dst.merge(right, arena);
        dst.deserialize_and_merge(*left_serialized, arena);
        dst.deserialize_and_merge(*right_serialized, arena);
    }
    EXPECT_EQ(arena.used_size(), payload(range(0, 10)));
    EXPECT_EQ(collect_set_result(*function, dst), sorted(values(range(0, 10))));
}

TEST(AggArenaDedupCoverageTest, CollectSetKeysOutliveInputColumnSourceStateAndBuffer) {
    auto function = make_function("collect_set", string_type());
    Arena arena;
    State dst(function);
    {
        auto rows = rows_of(range(0, 4), 5);
        dst.add({rows.get()}, arena);
        // Overwrite, then free the input bytes.
        auto& chars = rows->get_chars();
        std::memset(chars.data(), 'Z', chars.size());
    }
    {
        auto rows = rows_of(range(4, 8), 5);
        Arena source_arena;
        State source(function);
        source.add({rows.get()}, source_arena);
        dst.merge(source, arena);
    } // source state, its arena and its input are destroyed here
    {
        auto rows = rows_of(range(8, 12), 5);
        Arena source_arena;
        State source(function);
        source.add({rows.get()}, source_arena);
        auto serialized = source.serialize();
        dst.deserialize_and_merge(*serialized, arena);
        auto& chars = serialized->get_chars();
        std::memset(chars.data(), 'Z', chars.size());
    } // serialized buffer destroyed here
    EXPECT_EQ(arena.used_size(), payload(range(0, 12)));
    EXPECT_EQ(collect_set_result(*function, dst), sorted(values(range(0, 12))));
}

TEST(AggArenaDedupCoverageTest, CollectSetNullAndEmptyStrings) {
    auto function = make_function("collect_set", {make_nullable(std::make_shared<DataTypeString>())});
    auto strings = ColumnString::create();
    auto nulls = ColumnUInt8::create();
    const std::vector<std::pair<std::string, bool>> pattern = {
            {"", false}, {"", true}, {value(1), false}, {"", false}, {"", true}};
    for (int r = 0; r < 1000; ++r) {
        for (const auto& [s, is_null] : pattern) {
            strings->insert_data(s.data(), s.size());
            nulls->insert_value(is_null);
        }
    }
    auto column = ColumnNullable::create(std::move(strings), std::move(nulls));
    Arena arena;
    State state(function);
    state.add({column.get()}, arena);
    // NULLs are skipped; the empty string is kept once and has no payload.
    EXPECT_EQ(arena.used_size(), value(1).size());
    EXPECT_EQ(collect_set_result(*function, state), sorted({"", value(1)}));

    Arena empty_arena;
    AggregateFunctionCollectSetData<TYPE_STRING, false> data({});
    auto empties = constant_strings("", 1000);
    for (size_t row = 0; row < empties->size(); ++row) {
        data.add(*empties, row, empty_arena);
    }
    EXPECT_EQ(data.size(), 1);
    EXPECT_EQ(empty_arena.used_size(), 0);
}

// collect_set(s, max_size) with one below, exactly at and one above the cap, in add and merge.
TEST(AggArenaDedupCoverageTest, CollectSetMaxSizeBoundaries) {
    constexpr int32_t cap = 5;
    auto function = make_function(
            "collect_set", {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>()});
    for (size_t distinct : {size_t(cap - 1), size_t(cap), size_t(cap + 1)}) {
        SCOPED_TRACE(distinct);
        const auto ids = range(0, distinct);
        // Each value repeated before the next one appears, so the kept values are the first ones.
        auto rows = ColumnString::create();
        for (size_t id : ids) {
            for (int r = 0; r < 100; ++r) {
                const auto v = value(id);
                rows->insert_data(v.data(), v.size());
            }
        }
        auto caps = ColumnInt32::create(rows->size(), cap);
        const auto kept = range(0, std::min<size_t>(distinct, cap));

        Arena arena;
        State state(function);
        state.add({rows.get(), caps.get()}, arena);
        EXPECT_EQ(arena.used_size(), payload(kept));
        EXPECT_EQ(collect_set_result(*function, state), sorted(values(kept)));

        // Merging into a state that already holds two of the values.
        auto first_two = rows_of(range(0, 2), 50);
        auto first_two_caps = ColumnInt32::create(first_two->size(), cap);
        Arena dst_arena;
        State dst(function);
        dst.add({first_two.get(), first_two_caps.get()}, dst_arena);
        for (int i = 0; i < 10; ++i) {
            dst.merge(state, dst_arena);
            dst.deserialize_and_merge(*state.serialize(), dst_arena);
        }
        EXPECT_EQ(collect_set_result(*function, dst).size(), kept.size());
        EXPECT_EQ(dst_arena.used_size(), payload(kept));
    }
}

TEST(AggArenaDedupCoverageTest, CollectSetAllUniqueAndChunkGrowth) {
    auto function = make_function("collect_set", string_type());
    const auto ids = range(0, 3000);
    auto unique_rows = rows_of(ids, 1);
    auto repeated_rows = rows_of(ids, 3);
    for (auto* rows : {unique_rows.get(), repeated_rows.get()}) {
        // A 64-byte first chunk: the keys need many chunks of growing size.
        Arena arena(64);
        State state(function);
        state.add({rows}, arena);
        EXPECT_EQ(arena.used_size(), payload(ids));
        EXPECT_GT(arena.size(), 64);
        EXPECT_EQ(collect_set_result(*function, state), sorted(values(ids)));
    }
}

// --- multi_distinct_<fn> (AggregateFunctionDistinct) -------------------------------------

TEST(AggArenaDedupCoverageTest, DistinctRepeatedOverlappingMergeAndDeserializeAndMerge) {
    auto function = make_function("multi_distinct_group_concat", string_type());
    const auto left_ids = range(0, 7);
    const auto right_ids = range(3, 10);
    auto left_rows = rows_of(left_ids, 3);
    auto right_rows = rows_of(right_ids, 3);
    Arena left_arena;
    Arena right_arena;
    State left(function);
    State right(function);
    left.add({left_rows.get()}, left_arena);
    right.add({right_rows.get()}, right_arena);
    EXPECT_EQ(left_arena.used_size(), payload(left_ids));
    auto left_serialized = left.serialize();
    auto right_serialized = right.serialize();

    Arena arena;
    State dst(function);
    for (int i = 0; i < 200; ++i) {
        dst.merge(left, arena);
        dst.merge(right, arena);
        dst.deserialize_and_merge(*left_serialized, arena);
        dst.deserialize_and_merge(*right_serialized, arena);
    }
    EXPECT_EQ(arena.used_size(), payload(range(0, 10)));
    EXPECT_EQ(group_concat_result(*function, dst, ","), sorted(values(range(0, 10))));
}

TEST(AggArenaDedupCoverageTest, DistinctKeysOutliveInputColumnSourceStateAndBuffer) {
    for (bool two_arguments : {false, true}) {
        SCOPED_TRACE(two_arguments);
        DataTypes types = string_type();
        if (two_arguments) {
            types.push_back(std::make_shared<DataTypeString>());
        }
        auto function = make_function("multi_distinct_group_concat", types);
        const std::string sep = two_arguments ? "|" : ",";
        auto add_ids = [&](State& state, const std::vector<size_t>& ids, Arena& arena) {
            auto rows = rows_of(ids, 4);
            auto seps = constant_strings(sep, rows->size());
            if (two_arguments) {
                state.add({rows.get(), seps.get()}, arena);
            } else {
                state.add({rows.get()}, arena);
            }
            auto& chars = rows->get_chars();
            std::memset(chars.data(), 'Z', chars.size());
        };
        Arena arena;
        State dst(function);
        add_ids(dst, range(0, 4), arena);
        {
            Arena source_arena;
            State source(function);
            add_ids(source, range(4, 8), source_arena);
            dst.merge(source, arena);
        }
        {
            Arena source_arena;
            State source(function);
            add_ids(source, range(8, 12), source_arena);
            auto serialized = source.serialize();
            dst.deserialize_and_merge(*serialized, arena);
            auto& chars = serialized->get_chars();
            std::memset(chars.data(), 'Z', chars.size());
        }
        EXPECT_EQ(group_concat_result(*function, dst, sep), sorted(values(range(0, 12))));
    }
}

TEST(AggArenaDedupCoverageTest, DistinctMultipleArgumentsKeepOneKeyPerDistinctTuple) {
    DataTypes types {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()};
    auto function = make_function("multi_distinct_group_concat", types);
    const auto ids = range(0, 20);
    auto distinct_rows = rows_of(ids, 1);
    auto distinct_seps = constant_strings("|", distinct_rows->size());
    auto rows = rows_of(ids, 50);
    auto seps = constant_strings("|", rows->size());

    Arena reference_arena;
    State reference(function);
    reference.add({distinct_rows.get(), distinct_seps.get()}, reference_arena);

    Arena arena;
    State state(function);
    state.add({rows.get(), seps.get()}, arena);
    EXPECT_EQ(arena.used_size(), reference_arena.used_size());
    EXPECT_EQ(group_concat_result(*function, state, "|"), sorted(values(ids)));
}

TEST(AggArenaDedupCoverageTest, DistinctNullAndEmptyStrings) {
    auto function = make_function("multi_distinct_group_concat",
                                  {make_nullable(std::make_shared<DataTypeString>())});
    auto strings = ColumnString::create();
    auto nulls = ColumnUInt8::create();
    const std::vector<std::pair<std::string, bool>> pattern = {
            {"", false}, {"", true}, {value(2), false}, {"", false}};
    for (int r = 0; r < 1000; ++r) {
        for (const auto& [s, is_null] : pattern) {
            strings->insert_data(s.data(), s.size());
            nulls->insert_value(is_null);
        }
    }
    auto column = ColumnNullable::create(std::move(strings), std::move(nulls));
    Arena arena;
    State state(function);
    state.add({column.get()}, arena);
    EXPECT_EQ(arena.used_size(), value(2).size());
    auto to = ColumnString::create();
    function->insert_result_into(state.place, *to);
    // The empty string is one distinct key; group_concat joins it and value(2) in either order.
    const auto result = to->get_data_at(0).to_string();
    EXPECT_TRUE(result == "," + value(2) || result == value(2) + ",") << result;
}

template <bool stable>
void check_single_all_unique_and_chunk_growth() {
    const auto ids = range(0, 3000);
    auto rows = rows_of(ids, 3);
    const IColumn* columns[] = {rows.get()};
    Arena arena(64);
    AggregateFunctionDistinctSingleGenericData<stable> data;
    for (size_t row = 0; row < rows->size(); ++row) {
        data.add(columns, 1, row, arena);
    }
    EXPECT_EQ(arena.used_size(), payload(ids));
    auto result = data.get_arguments(string_type());
    std::vector<std::string> got;
    for (size_t i = 0; i < result[0]->size(); ++i) {
        got.push_back(result[0]->get_data_at(i).to_string());
    }
    if constexpr (stable) {
        EXPECT_EQ(got, values(ids));
    } else {
        EXPECT_EQ(sorted(got), sorted(values(ids)));
    }
}

TEST(AggArenaDedupCoverageTest, DistinctSingleAllUniqueAndChunkGrowth) {
    check_single_all_unique_and_chunk_growth<false>();
    check_single_all_unique_and_chunk_growth<true>();
}

TEST(AggArenaDedupCoverageTest, DistinctMultipleAllUniqueInput) {
    const auto ids = range(0, 3000);
    auto rows = rows_of(ids, 1);
    auto numbers = ColumnInt32::create();
    for (size_t i = 0; i < rows->size(); ++i) {
        numbers->insert_value(static_cast<int32_t>(i));
    }
    const IColumn* columns[] = {rows.get(), numbers.get()};
    size_t expected_bytes = 0;
    {
        Arena scratch;
        for (size_t row = 0; row < rows->size(); ++row) {
            const char* begin = nullptr;
            expected_bytes += rows->serialize_value_into_arena(row, scratch, begin).size;
            expected_bytes += numbers->serialize_value_into_arena(row, scratch, begin).size;
        }
    }
    Arena arena;
    AggregateFunctionDistinctMultipleGenericData<false> data;
    for (size_t row = 0; row < rows->size(); ++row) {
        data.add(columns, 2, row, arena);
    }
    EXPECT_EQ(data.data.size(), ids.size());
    // No duplicates: every key is kept, and keys that crossed a chunk boundary may leave a
    // copied prefix behind, so the arena holds at least the payload.
    EXPECT_GE(arena.used_size(), expected_bytes);
    EXPECT_LT(arena.used_size(), expected_bytes + 64 * 1024);
}

// The stable form maps each key to the order of its first insertion; duplicates keep it.
TEST(AggArenaDedupCoverageTest, StableDistinctKeepsFirstInsertionIndex) {
    const std::vector<size_t> order = {3, 1, 3, 2, 1, 0, 2, 3, 0};
    const std::vector<size_t> first_seen = {3, 1, 2, 0};
    auto rows = ColumnString::create();
    auto numbers = ColumnInt32::create();
    for (size_t id : order) {
        const auto v = value(id);
        rows->insert_data(v.data(), v.size());
        numbers->insert_value(static_cast<int32_t>(id));
    }
    const IColumn* columns[] = {rows.get(), numbers.get()};

    Arena single_arena;
    AggregateFunctionDistinctSingleGenericData<true> single;
    for (size_t row = 0; row < rows->size(); ++row) {
        single.add(columns, 1, row, single_arena);
    }
    for (size_t index = 0; index < first_seen.size(); ++index) {
        const auto v = value(first_seen[index]);
        auto it = single.data.find(StringRef(v.data(), v.size()));
        ASSERT_NE(it, single.data.end());
        EXPECT_EQ(it->second, index);
    }
    EXPECT_EQ(single_arena.used_size(), payload(first_seen));

    Arena multiple_arena;
    AggregateFunctionDistinctMultipleGenericData<true> multiple;
    for (size_t row = 0; row < rows->size(); ++row) {
        multiple.add(columns, 2, row, multiple_arena);
    }
    DataTypes types {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>()};
    auto result = multiple.get_arguments(types);
    ASSERT_EQ(result[1]->size(), first_seen.size());
    for (size_t index = 0; index < first_seen.size(); ++index) {
        EXPECT_EQ(result[1]->get_int(index), static_cast<int64_t>(first_seen[index]));
        EXPECT_EQ(result[0]->get_data_at(index).to_string(), value(first_seen[index]));
    }
}

} // namespace
} // namespace doris
