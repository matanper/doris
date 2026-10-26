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
#include <bvar/bvar.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include "common/config.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/string_buffer.hpp"
#include "exec/common/variant_util.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "storage/data_dir.h"
#include "storage/merger.h"
#include "storage/predicate/column_predicate.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/segment/segment_loader.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include <fstream>
#include <iterator>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <time.h>
#include <set>
#include <stdexcept>
#include <thread>

#include "core/column/column_variant.h"
#include "core/data_type/data_type_variant.h"
#include "storage/index/index_writer.h"
#include "storage/options.h"
#include "storage/segment/segment_write_tasks.h"

// Writes and compacts segments shaped like a log table, with several VARIANT columns holding
// hundreds of subcolumns, typed and sparse paths and an inverted index, with the segment write
// parallelism on and off, and checks that every segment file is the same.
namespace doris {
namespace {

int64_t exposed(const std::string& name) {
    return std::stoll(bvar::Variable::describe_exposed(name));
}

constexpr const char* kStressDir = "./ut_dir/parallel_segment_write_stress_test";

std::string quoted(const std::string& v) {
    return "\"" + v + "\"";
}

// An event with ~25 of 400 keys of mixed types, nested objects, the typed `message` and
// `class_uid`, and a long tail.
std::string event_json(int64_t row, std::mt19937_64& rng) {
    std::string json = "{\"class_uid\":" + std::to_string(1000 + rng() % 7) +
                       ",\"message\":" + quoted("user u" + std::to_string(rng() % 300) +
                                                " logged in from host-" + std::to_string(row % 17));
    const uint64_t first = rng() % 400;
    for (uint64_t i = 0; i < 25; ++i) {
        const uint64_t key = (first + i * 17) % 400;
        json += ",\"k" + std::to_string(key) + "\":";
        switch (key % 4) {
        case 0:
            json += std::to_string(rng() % 100000);
            break;
        case 1:
            json += quoted("v" + std::to_string(rng() % 1000));
            break;
        case 2:
            json += "{\"a\":" + std::to_string(rng() % 10) + ",\"b\":" +
                    quoted("x" + std::to_string(rng() % 20)) + "}";
            break;
        default:
            json += "[" + std::to_string(rng() % 5) + "," + std::to_string(rng() % 5) + "]";
            break;
        }
    }
    return json + "}";
}

std::string tags_json(std::mt19937_64& rng, int keys, int names) {
    std::string json = "{";
    const uint64_t first = rng() % names;
    for (int i = 0; i < keys; ++i) {
        json += (i ? ",\"t" : "\"t") + std::to_string((first + 7 * i) % names) +
                "\":" + quoted("tag" + std::to_string(rng() % 50));
    }
    return json + "}";
}

std::string read_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

template <typename T>
class ScopedValue {
public:
    ScopedValue(T* field, T value) : _field(field), _old(*field) { *_field = value; }
    ~ScopedValue() { *_field = _old; }

private:
    T* _field;
    T _old;
};

} // namespace

class ParallelSegmentWriteStressTest : public testing::Test {
protected:
    void SetUp() override {
        _root = std::filesystem::absolute(std::string(kStressDir)).string();
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_root).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_root).ok());
        auto engine = std::make_unique<StorageEngine>(EngineOptions {});
        _engine = engine.get();
        _data_dir = std::make_unique<DataDir>(*_engine, _root);
        static_cast<void>(_data_dir->update_capacity());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
        if (ExecEnv::GetInstance()->get_tmp_file_dirs() == nullptr) {
            std::vector<StorePath> paths;
            paths.emplace_back(_root + "/tmp_store", -1);
            auto tmp_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
            ASSERT_TRUE(tmp_dirs->init().ok());
            ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_dirs));
        }
    }

    void TearDown() override {
        _tablet.reset();
        _data_dir.reset();
        _engine = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_root).ok());
    }

    static void add_variant(TabletSchemaPB* pb, int32_t uid, const std::string& name) {
        ColumnPB* c = pb->add_column();
        c->set_unique_id(uid);
        c->set_name(name);
        c->set_type("VARIANT");
        c->set_is_key(false);
        c->set_is_nullable(true);
        c->set_aggregation("NONE");
        c->set_variant_max_subcolumns_count(2048);
        c->set_variant_enable_typed_paths_to_sparse(false);
        c->set_variant_max_sparse_column_statistics_size(10000);
        c->set_variant_sparse_hash_shard_count(1);
    }

    static void add_string(TabletSchemaPB* pb, int32_t uid, const std::string& name) {
        ColumnPB* c = pb->add_column();
        c->set_unique_id(uid);
        c->set_name(name);
        c->set_type("STRING");
        c->set_is_key(false);
        c->set_is_nullable(true);
        c->set_length(2147483643);
        c->set_aggregation("NONE");
    }

    // ts, org, raw (indexed), event (VARIANT with typed, indexed paths), source, tags,
    // internal_tags, log_source: the variant columns sit between plain ones, as in a log table.
    void create_tablet(int64_t tablet_id) {
        TabletSchemaPB pb;
        pb.set_keys_type(KeysType::DUP_KEYS);
        pb.set_compression_type(segment_v2::CompressionTypePB::ZSTD);
        pb.set_num_short_key_columns(1);
        pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V3);
        ColumnPB* key = pb.add_column();
        key->set_unique_id(0);
        key->set_name("ts");
        key->set_type("BIGINT");
        key->set_is_key(true);
        key->set_is_nullable(false);
        key->set_length(8);
        add_string(&pb, 1, "org");
        add_string(&pb, 2, "raw");
        add_variant(&pb, 3, "event");
        for (const auto& [path, type] : std::vector<std::pair<std::string, std::string>> {
                     {"message", "STRING"}, {"class_uid", "BIGINT"}}) {
            ColumnPB* typed = pb.mutable_column(3)->add_children_columns();
            typed->set_unique_id(-1);
            typed->set_name(path);
            typed->set_type(type);
            typed->set_is_key(false);
            typed->set_is_nullable(true);
            typed->set_pattern_type(PatternTypePB::MATCH_NAME);
            typed->set_parent_unique_id(3);
        }
        add_string(&pb, 4, "source");
        add_variant(&pb, 5, "tags");
        add_variant(&pb, 6, "internal_tags");
        add_string(&pb, 7, "log_source");
        auto* raw_index = pb.add_index();
        raw_index->set_index_id(9001);
        raw_index->set_index_name("idx_raw");
        raw_index->set_index_type(IndexType::INVERTED);
        raw_index->add_col_unique_id(2);
        (*raw_index->mutable_properties())["parser"] = "english";
        auto* message_index = pb.add_index();
        message_index->set_index_id(9002);
        message_index->set_index_name("idx_message");
        message_index->set_index_type(IndexType::INVERTED);
        message_index->add_col_unique_id(3);
        (*message_index->mutable_properties())["field_pattern"] = "message";
        (*message_index->mutable_properties())["parser"] = "english";
        auto* class_index = pb.add_index();
        class_index->set_index_id(9003);
        class_index->set_index_name("idx_class_uid");
        class_index->set_index_type(IndexType::INVERTED);
        class_index->add_col_unique_id(3);
        (*class_index->mutable_properties())["field_pattern"] = "class_uid";
        _schema = std::make_shared<TabletSchema>();
        _schema->init_from_pb(pb);
        TabletMetaSharedPtr tablet_meta(new TabletMeta(_schema));
        tablet_meta->_tablet_id = tablet_id;
        _tablet = std::make_shared<Tablet>(*_engine, tablet_meta, _data_dir.get());
        ASSERT_TRUE(_tablet->init().ok());
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    }

    struct InputRows {
        std::vector<int64_t> ts;
        std::vector<std::string> org, raw, source, log_source;
        std::vector<std::optional<std::string>> event, tags, internal_tags;
    };

    static InputRows make_rows(int64_t rowset, int64_t rows) {
        InputRows in;
        std::mt19937_64 rng(1000 + rowset);
        for (int64_t row = 0; row < rows; ++row) {
            in.ts.push_back(rowset * rows + row);
            in.org.push_back("org-" + std::to_string(row % 5));
            in.raw.push_back("2026-10-20 event " + std::to_string(rng() % 1000) + " user u" +
                             std::to_string(rng() % 300) + " action " +
                             std::to_string(rng() % 9));
            in.source.push_back("src" + std::to_string(row % 3));
            in.log_source.push_back("ls" + std::to_string(row % 4));
            in.event.push_back(row % 13 == 7 ? std::nullopt
                                             : std::optional<std::string>(event_json(row, rng)));
            in.tags.push_back(std::optional<std::string>(tags_json(rng, 5, 40)));
            in.internal_tags.push_back(row % 5 == 0 ? std::nullopt
                                                    : std::optional<std::string>(
                                                              tags_json(rng, 4, 30)));
        }
        return in;
    }

    // The VARIANT columns are written as V2 (ColumnVariantV2) or V1 (ColumnVariant) data.
    ColumnWithTypeAndName variant_column(const std::vector<std::optional<std::string>>& jsons,
                                         int cid, bool v2) {
        auto nulls = ColumnUInt8::create();
        const int32_t max_subcolumns = _schema->column(cid).variant_max_subcolumns_count();
        if (v2) {
            auto values = ColumnVariantV2::create();
            DataTypeVariantV2SerDe serde;
            DataTypeSerDe::FormatOptions options;
            for (const auto& json : jsons) {
                if (json.has_value()) {
                    Slice slice(json->data(), json->size());
                    auto st = serde.deserialize_one_cell_from_json(*values, slice, options);
                    EXPECT_TRUE(st.ok()) << st << " " << *json;
                    nulls->insert_value(0);
                } else {
                    values->insert_default();
                    nulls->insert_value(1);
                }
            }
            return {ColumnNullable::create(std::move(values), std::move(nulls)),
                    make_nullable(std::make_shared<DataTypeVariantV2>(max_subcolumns)),
                    _schema->column(cid).name()};
        }
        auto strings = ColumnString::create();
        for (const auto& json : jsons) {
            const std::string text = json.value_or("{}");
            strings->insert_data(text.data(), text.size());
            nulls->insert_value(json.has_value() ? 0 : 1);
        }
        auto values = ColumnVariant::create(max_subcolumns, false);
        ParseConfig config;
        variant_util::parse_json_to_variant(*values, *strings, config);
        return {ColumnNullable::create(std::move(values), std::move(nulls)),
                make_nullable(std::make_shared<DataTypeVariant>(max_subcolumns)),
                _schema->column(cid).name()};
    }

    static ColumnWithTypeAndName string_column(const std::vector<std::string>& values,
                                               const std::string& name) {
        auto strings = ColumnString::create();
        for (const auto& v : values) {
            strings->insert_data(v.data(), v.size());
        }
        auto nulls = ColumnUInt8::create(values.size(), 0);
        return {ColumnNullable::create(std::move(strings), std::move(nulls)),
                make_nullable(std::make_shared<DataTypeString>()), name};
    }

    RowsetSharedPtr write_rowset(const InputRows& in, int64_t version, bool v2) {
        Block block = make_block(in, v2);
        return write_block(&block, version);
    }

    Block make_block(const InputRows& in, bool v2) {
        auto keys = ColumnInt64::create();
        for (auto ts : in.ts) {
            keys->insert_value(ts);
        }
        Block block;
        block.insert({std::move(keys), std::make_shared<DataTypeInt64>(), "ts"});
        block.insert(string_column(in.org, "org"));
        block.insert(string_column(in.raw, "raw"));
        block.insert(variant_column(in.event, 3, v2));
        block.insert(string_column(in.source, "source"));
        block.insert(variant_column(in.tags, 5, v2));
        block.insert(variant_column(in.internal_tags, 6, v2));
        block.insert(string_column(in.log_source, "log_source"));
        return block;
    }

    RowsetSharedPtr write_block(Block* block, int64_t version) {
        RowsetWriterContext ctx;
        RowsetId rowset_id;
        rowset_id.init(++_next_rowset);
        ctx.rowset_id = rowset_id;
        ctx.rowset_type = BETA_ROWSET;
        ctx.data_dir = _data_dir.get();
        ctx.rowset_state = VISIBLE;
        ctx.tablet_schema = _schema;
        ctx.tablet_path = _tablet->tablet_path();
        ctx.tablet_id = _tablet->tablet_id();
        ctx.tablet = _tablet;
        ctx.version = Version(version, version);
        ctx.segments_overlap = NONOVERLAPPING;
        ctx.max_rows_per_segment = 1 << 20;
        ctx.write_type = DataWriteType::TYPE_DIRECT;
        auto writer_result = RowsetFactory::create_rowset_writer(*_engine, ctx, false);
        EXPECT_TRUE(writer_result.has_value()) << writer_result.error();
        auto writer = std::move(writer_result).value();
        // flush_single_block is the memtable flush entry point, which writes through
        // VerticalSegmentWriter.
        Status st = writer->flush_single_block(block);
        EXPECT_TRUE(st.ok()) << st.to_string();
        RowsetSharedPtr rowset;
        st = writer->build(rowset);
        EXPECT_TRUE(st.ok()) << st.to_string();
        return rowset;
    }

    RowsetSharedPtr compact(const std::vector<RowsetSharedPtr>& inputs) {
        std::vector<RowsetReaderSharedPtr> readers;
        for (const auto& rowset : inputs) {
            RowsetReaderSharedPtr reader;
            EXPECT_TRUE(rowset->create_reader(&reader).ok());
            readers.push_back(std::move(reader));
        }
        auto schema = std::make_shared<TabletSchema>(*_schema);
        EXPECT_TRUE(variant_util::VariantCompactionUtil::get_extended_compaction_schema(inputs,
                                                                                        schema)
                            .ok());
        RowsetWriterContext ctx;
        RowsetId rowset_id;
        rowset_id.init(++_next_rowset);
        ctx.rowset_id = rowset_id;
        ctx.rowset_type = BETA_ROWSET;
        ctx.data_dir = _data_dir.get();
        ctx.rowset_state = VISIBLE;
        ctx.tablet_schema = schema;
        ctx.tablet_path = _tablet->tablet_path();
        ctx.tablet_id = _tablet->tablet_id();
        ctx.tablet = _tablet;
        ctx.version = Version(inputs.front()->start_version(), inputs.back()->end_version());
        ctx.segments_overlap = NONOVERLAPPING;
        ctx.write_type = DataWriteType::TYPE_COMPACTION;
        ctx.compaction_type = ReaderType::READER_CUMULATIVE_COMPACTION;
        auto writer_result = RowsetFactory::create_rowset_writer(*_engine, ctx, true);
        EXPECT_TRUE(writer_result.has_value()) << writer_result.error();
        auto writer = std::move(writer_result).value();
        Merger::Statistics stats;
        Status st = Merger::vertical_merge_rowsets(_tablet, ReaderType::READER_CUMULATIVE_COMPACTION,
                                                   *schema, readers, writer.get(), 1 << 20,
                                                   static_cast<int64_t>(inputs.size()), &stats);
        EXPECT_TRUE(st.ok()) << st.to_string();
        RowsetSharedPtr output;
        st = writer->build(output);
        EXPECT_TRUE(st.ok()) << st.to_string();
        return output;
    }

    static std::string segment_bytes(const RowsetSharedPtr& rowset) {
        if (rowset == nullptr) {
            return {};
        }
        auto path = rowset->segment_path(0);
        return path.has_value() ? read_file(path.value()) : std::string {};
    }

    void run(int64_t tablet_id, bool v2) {
        create_tablet(tablet_id);
        std::vector<InputRows> inputs;
        for (int64_t r = 0; r < 3; ++r) {
            inputs.push_back(make_rows(r, 1500));
        }
        std::vector<std::string> expected_loads;
        std::string expected_compaction;
        // Serial once for the reference, then parallel with the default task size, with every
        // column and subcolumn its own task, and with small multi-column chunks.
        const int64_t min_bytes[] = {config::segment_write_task_min_bytes,
                                     config::segment_write_task_min_bytes, 0, 16384, 0};
        for (int round = 0; round < 5; ++round) {
            const int32_t parallelism = round == 0 ? 1 : 8;
            ScopedValue columns(&config::segment_column_write_parallelism, parallelism);
            ScopedValue subcolumns(&config::variant_subcolumn_write_parallelism, parallelism);
            ScopedValue finish(&config::index_finish_parallelism, parallelism);
            ScopedValue task_bytes(&config::segment_write_task_min_bytes, min_bytes[round]);
            const int64_t tasks_before = exposed("segment_write_parallel_tasks");
            const int64_t inline_before = exposed("segment_write_inline_tasks");
            std::vector<RowsetSharedPtr> rowsets;
            for (int64_t r = 0; r < 3; ++r) {
                rowsets.push_back(write_rowset(inputs[r], 2 + r, v2));
                ASSERT_NE(rowsets.back(), nullptr);
                const std::string bytes = segment_bytes(rowsets.back());
                ASSERT_FALSE(bytes.empty());
                if (round == 0) {
                    expected_loads.push_back(bytes);
                } else {
                    EXPECT_TRUE(bytes == expected_loads[r])
                            << "load " << r << " round " << round << ": " << bytes.size()
                            << " vs " << expected_loads[r].size();
                }
            }
            const std::string compacted = segment_bytes(compact(rowsets));
            ASSERT_FALSE(compacted.empty());
            std::cout << "round " << round << " min_bytes=" << min_bytes[round] << " pool tasks="
                      << exposed("segment_write_parallel_tasks") - tasks_before
                      << " inline tasks=" << exposed("segment_write_inline_tasks") - inline_before
                      << std::endl;
            if (round == 0) {
                expected_compaction = compacted;
            } else {
                EXPECT_TRUE(compacted == expected_compaction)
                        << "compaction round " << round << ": " << compacted.size() << " vs "
                        << expected_compaction.size();
            }
        }
    }

    std::string _root;
    StorageEngine* _engine = nullptr;
    std::unique_ptr<DataDir> _data_dir;
    TabletSchemaSPtr _schema;
    TabletSharedPtr _tablet;
    std::atomic<int64_t> _next_rowset {900000};
};

TEST_F(ParallelSegmentWriteStressTest, VariantV2LogTableSegmentsAreIdentical) {
    run(820001, true);
}

TEST_F(ParallelSegmentWriteStressTest, VariantV1LogTableSegmentsAreIdentical) {
    run(820002, false);
}

static double process_cpu_seconds() {
    timespec ts {};
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts);
    return static_cast<double>(ts.tv_sec) + static_cast<double>(ts.tv_nsec) / 1e9;
}

static double wall_seconds() {
    return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch())
            .count();
}

// Load flush and compaction CPU, wall time and pool tasks per row of the log table, written
// serially, with every column and subcolumn a pool task, and with the default task size. The
// flushes write one memtable-sized block each; the compaction merges in its own block size.
// Runs only when PARALLEL_WRITE_BENCH_ROWS gives the rows per flush.
TEST_F(ParallelSegmentWriteStressTest, LoadAndCompactionCost) {
    const char* rows_env = std::getenv("PARALLEL_WRITE_BENCH_ROWS");
    if (rows_env == nullptr) {
        GTEST_SKIP() << "set PARALLEL_WRITE_BENCH_ROWS to run";
    }
    const int64_t rows = std::atoll(rows_env);
    struct Mode {
        const char* name;
        int32_t parallelism;
        int64_t min_bytes;
    };
    const Mode modes[] = {{"serial", 1, config::segment_write_task_min_bytes},
                          {"per-column-tasks", 8, 0},
                          {"chunked", 8, config::segment_write_task_min_bytes}};
    for (bool v2 : {false, true}) {
        create_tablet(v2 ? 830002 : 830001);
        std::vector<InputRows> inputs;
        for (int64_t r = 0; r < 3; ++r) {
            inputs.push_back(make_rows(r, rows));
        }
        for (int repeat = 0; repeat < 2; ++repeat) {
            for (const Mode& mode : modes) {
                ScopedValue columns(&config::segment_column_write_parallelism, mode.parallelism);
                ScopedValue subcolumns(&config::variant_subcolumn_write_parallelism,
                                       mode.parallelism);
                ScopedValue finish(&config::index_finish_parallelism, mode.parallelism);
                ScopedValue task_bytes(&config::segment_write_task_min_bytes, mode.min_bytes);
                std::vector<Block> blocks;
                for (const auto& in : inputs) {
                    blocks.push_back(make_block(in, v2));
                }
                int64_t tasks = exposed("segment_write_parallel_tasks");
                double cpu = process_cpu_seconds();
                double wall = wall_seconds();
                std::vector<RowsetSharedPtr> rowsets;
                for (int64_t r = 0; r < 3; ++r) {
                    rowsets.push_back(write_block(&blocks[r], 2 + r));
                    ASSERT_NE(rowsets.back(), nullptr);
                }
                const double load_cpu = process_cpu_seconds() - cpu;
                const double load_wall = wall_seconds() - wall;
                const int64_t load_tasks = exposed("segment_write_parallel_tasks") - tasks;
                tasks = exposed("segment_write_parallel_tasks");
                cpu = process_cpu_seconds();
                wall = wall_seconds();
                auto output = compact(rowsets);
                ASSERT_NE(output, nullptr);
                const double compaction_cpu = process_cpu_seconds() - cpu;
                const double compaction_wall = wall_seconds() - wall;
                const int64_t compaction_tasks = exposed("segment_write_parallel_tasks") - tasks;
                const auto total = static_cast<double>(3 * rows);
                std::cout << "WRITE_BENCH variant=" << (v2 ? "v2" : "v1") << " mode=" << mode.name
                          << " load_cpu_s=" << load_cpu << " load_wall_s=" << load_wall
                          << " load_tasks_per_krow=" << load_tasks * 1000.0 / total
                          << " compaction_cpu_s=" << compaction_cpu
                          << " compaction_wall_s=" << compaction_wall
                          << " compaction_tasks_per_krow=" << compaction_tasks * 1000.0 / total
                          << std::endl;
            }
        }
    }
}

// Memtable flush wall time, p50 / p90 / max over many flushes, while another thread compacts
// continuously, for each write mode. Runs only when PARALLEL_WRITE_BENCH_ROWS gives the rows per
// flush.
TEST_F(ParallelSegmentWriteStressTest, FlushTailUnderCompaction) {
    const char* rows_env = std::getenv("PARALLEL_WRITE_BENCH_ROWS");
    if (rows_env == nullptr) {
        GTEST_SKIP() << "set PARALLEL_WRITE_BENCH_ROWS to run";
    }
    const int64_t rows = std::atoll(rows_env);
    const int flushes = 24;
    struct Mode {
        const char* name;
        int32_t parallelism;
        int64_t min_bytes;
    };
    const Mode modes[] = {{"serial", 1, config::segment_write_task_min_bytes},
                          {"per-column-tasks", 8, 0},
                          {"chunked", 8, config::segment_write_task_min_bytes}};
    create_tablet(830003);
    std::vector<InputRows> inputs;
    for (int64_t r = 0; r < 4; ++r) {
        inputs.push_back(make_rows(r, rows));
    }
    std::vector<RowsetSharedPtr> compaction_inputs;
    for (int64_t r = 0; r < 3; ++r) {
        compaction_inputs.push_back(write_rowset(inputs[r], 2 + r, false));
        ASSERT_NE(compaction_inputs.back(), nullptr);
    }
    for (const Mode& mode : modes) {
        ScopedValue columns(&config::segment_column_write_parallelism, mode.parallelism);
        ScopedValue subcolumns(&config::variant_subcolumn_write_parallelism, mode.parallelism);
        ScopedValue finish(&config::index_finish_parallelism, mode.parallelism);
        ScopedValue task_bytes(&config::segment_write_task_min_bytes, mode.min_bytes);
        std::atomic<bool> stop {false};
        std::atomic<int> compactions {0};
        std::vector<std::thread> compactors;
        for (int t = 0; t < 4; ++t) {
            compactors.emplace_back([&]() {
                SCOPED_INIT_THREAD_CONTEXT();
                while (!stop.load()) {
                    if (compact(compaction_inputs) != nullptr) {
                        ++compactions;
                    }
                }
            });
        }
        std::vector<double> walls;
        for (int i = 0; i < flushes; ++i) {
            Block block = make_block(inputs[3], false);
            const double start = wall_seconds();
            auto rowset = write_block(&block, 10 + i);
            walls.push_back((wall_seconds() - start) * 1000);
            ASSERT_NE(rowset, nullptr);
        }
        stop = true;
        for (auto& t : compactors) {
            t.join();
        }
        std::sort(walls.begin(), walls.end());
        std::cout << "FLUSH_TAIL mode=" << mode.name << " rows=" << rows
                  << " p50_ms=" << walls[walls.size() / 2]
                  << " p90_ms=" << walls[walls.size() * 9 / 10] << " max_ms=" << walls.back()
                  << " concurrent_compactions=" << compactions.load() << std::endl;
    }
}

// Tasks smaller than a chunk together run on the calling thread, in order, with no pool task.
TEST(ChunkedSegmentWriteTasksTest, SmallTasksRunOnTheCallingThread) {
    ScopedValue min_bytes(&config::segment_write_task_min_bytes, int64_t {1000});
    const int64_t pool_before = exposed("segment_write_parallel_tasks");
    std::vector<int> order;
    const auto caller = std::this_thread::get_id();
    bool on_caller = true;
    segment_v2::ChunkedSegmentWriteTasks tasks(8);
    for (int i = 0; i < 10; ++i) {
        tasks.add(100, [&, i]() {
            order.push_back(i);
            on_caller = on_caller && std::this_thread::get_id() == caller;
            return Status::OK();
        });
    }
    EXPECT_TRUE(order.empty());
    ASSERT_TRUE(tasks.wait().ok());
    EXPECT_EQ(order, (std::vector<int> {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    EXPECT_TRUE(on_caller);
    EXPECT_EQ(exposed("segment_write_parallel_tasks") - pool_before, 0);
}

// Tasks are grouped into chunks of at least segment_write_task_min_bytes, one pool task each,
// and every task runs once; a failing task fails the run and skips the rest of its chunk.
TEST(ChunkedSegmentWriteTasksTest, LargeInputsRunInChunks) {
    ScopedValue min_bytes(&config::segment_write_task_min_bytes, int64_t {1000});
    const int64_t pool_before = exposed("segment_write_parallel_tasks");
    std::atomic<int> ran {0};
    segment_v2::ChunkedSegmentWriteTasks tasks(8);
    for (int i = 0; i < 40; ++i) {
        tasks.add(250, [&ran]() {
            ++ran;
            return Status::OK();
        });
    }
    ASSERT_TRUE(tasks.wait().ok());
    EXPECT_EQ(ran, 40);
    EXPECT_EQ(exposed("segment_write_parallel_tasks") - pool_before, 10);

    std::atomic<int> ran_after_failure {0};
    segment_v2::ChunkedSegmentWriteTasks failing(8);
    for (int i = 0; i < 8; ++i) {
        failing.add(250, [&ran_after_failure, i]() -> Status {
            if (i == 4) {
                return Status::InternalError("injected");
            }
            if (i > 4 && i < 8) {
                ++ran_after_failure;
            }
            return Status::OK();
        });
    }
    EXPECT_FALSE(failing.wait().ok());
    EXPECT_EQ(ran_after_failure, 0);

    segment_v2::ChunkedSegmentWriteTasks serial(1);
    int serial_ran = 0;
    for (int i = 0; i < 3; ++i) {
        serial.add(5000, [&serial_ran]() {
            ++serial_ran;
            return Status::OK();
        });
        EXPECT_EQ(serial_ran, i + 1);
    }
    EXPECT_TRUE(serial.wait().ok());
}

// A task that throws on a pool thread fails its group with the exception's status, instead of
// ending the process, and the other tasks still run.
TEST(SegmentWriteTaskGroupTest, ThrowingTasksFailTheGroup) {
    std::atomic<int> ran {0};
    segment_v2::SegmentWriteTaskGroup group(8);
    for (int i = 0; i < 32; ++i) {
        group.add([&ran, i]() -> Status {
            ++ran;
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            if (i == 5) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "injected");
            }
            if (i == 9) {
                throw std::runtime_error("injected std");
            }
            return Status::OK();
        });
    }
    Status st = group.wait();
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>() || st.is<ErrorCode::INTERNAL_ERROR>()) << st;
    EXPECT_EQ(ran, 32);
    EXPECT_FALSE(segment_v2::run_catching_exceptions([]() -> Status { throw 7; }).ok());
}

} // namespace doris
