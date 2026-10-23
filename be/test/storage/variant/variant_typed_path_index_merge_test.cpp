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

#include <CLucene.h>
#include <CLucene/config/repl_wchar.h>
#include <time.h>
#include <bvar/bvar.h>
#include <gtest/gtest.h>
#include <roaring/roaring.hh>

#include <chrono>
#include <cstdlib>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "common/config.h"
#include "util/defer_op.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/column/column_nullable.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "storage/index/index_file_reader.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/variant/index_storage_variant_test_base.h"

namespace doris::index_storage_test {
namespace {

constexpr int32_t kVariantUid = 2;

int64_t exposed(const std::string& name) {
    return std::stoll(bvar::Variable::describe_exposed(name));
}

// Every term of `index` with its documents, frequencies and positions.
std::map<std::string, std::vector<std::pair<int32_t, std::vector<int32_t>>>> postings(
        lucene::store::Directory* dir) {
    std::map<std::string, std::vector<std::pair<int32_t, std::vector<int32_t>>>> out;
    lucene::index::IndexReader* reader = lucene::index::IndexReader::open(dir);
    lucene::index::TermEnum* terms = reader->terms();
    while (terms->next()) {
        lucene::index::Term* term = terms->term();
        std::string key = lucene_wcstoutf8string(term->field(), wcslen(term->field())) + ":" +
                          lucene_wcstoutf8string(term->text(), term->textLength());
        lucene::index::TermPositions* positions = reader->termPositions(term);
        auto& docs = out[key];
        while (positions->next()) {
            std::vector<int32_t> pos;
            for (int32_t i = 0; i < positions->freq(); ++i) {
                pos.push_back(positions->nextPosition());
            }
            docs.emplace_back(positions->doc(), std::move(pos));
        }
        positions->close();
        _CLLDELETE(positions);
        _CLDECDELETE(term);
    }
    terms->close();
    _CLLDELETE(terms);
    reader->close();
    _CLLDELETE(reader);
    return out;
}

// ExecEnv creates its IndexPolicyMgr only in ExecEnv::init, which unit tests do not run, and
// offers no setter. An explicit instantiation may name a private member.
template <typename Tag, typename Tag::type Member>
struct PrivateMemberAccess {
    friend typename Tag::type private_member(Tag) { return Member; }
};
struct ExecEnvIndexPolicyMgrTag {
    using type = IndexPolicyMgr* ExecEnv::*;
    friend type private_member(ExecEnvIndexPolicyMgrTag);
};
template struct PrivateMemberAccess<ExecEnvIndexPolicyMgrTag, &ExecEnv::_index_policy_mgr>;

// Installs the custom analyzers alnum_lower and ngram4_lower for the object's lifetime.
class ScopedAnalyzers {
public:
    ScopedAnalyzers() {
        TIndexPolicy alnum;
        alnum.id = 1;
        alnum.name = "alnum_lower";
        alnum.type = TIndexPolicyType::ANALYZER;
        alnum.properties["tokenizer"] = "basic";
        alnum.properties["token_filter"] = "lowercase";
        TIndexPolicy ngram;
        ngram.id = 2;
        ngram.name = "ngram4";
        ngram.type = TIndexPolicyType::TOKENIZER;
        ngram.properties["type"] = "ngram";
        ngram.properties["min_gram"] = "4";
        ngram.properties["max_gram"] = "4";
        TIndexPolicy ngram_lower;
        ngram_lower.id = 3;
        ngram_lower.name = "ngram4_lower";
        ngram_lower.type = TIndexPolicyType::ANALYZER;
        ngram_lower.properties["tokenizer"] = "ngram4";
        ngram_lower.properties["token_filter"] = "lowercase";
        _mgr.apply_policy_changes({alnum, ngram, ngram_lower}, {});
        auto& slot = ExecEnv::GetInstance()->*private_member(ExecEnvIndexPolicyMgrTag {});
        _previous = slot;
        slot = &_mgr;
    }
    ~ScopedAnalyzers() {
        ExecEnv::GetInstance()->*private_member(ExecEnvIndexPolicyMgrTag {}) = _previous;
    }

private:
    IndexPolicyMgr _mgr;
    IndexPolicyMgr* _previous = nullptr;
};

// The null documents of an index, empty when it has no null_bitmap file.
roaring::Roaring null_bitmap(lucene::store::Directory* dir) {
    roaring::Roaring bitmap;
    if (!dir->fileExists("null_bitmap")) {
        return bitmap;
    }
    lucene::store::IndexInput* in = dir->openInput("null_bitmap");
    std::string buf(static_cast<size_t>(in->length()), '\0');
    if (!buf.empty()) {
        in->readBytes(reinterpret_cast<uint8_t*>(buf.data()), static_cast<int32_t>(buf.size()));
        bitmap = roaring::Roaring::read(buf.data(), false);
    }
    in->close();
    _CLLDELETE(in);
    return bitmap;
}

// A VARIANT V2 column of the given JSON objects.
ColumnPtr variant_v2_column(const std::vector<std::string>& jsons) {
    auto values = ColumnVariantV2::create();
    DataTypeVariantV2SerDe serde;
    DataTypeSerDe::FormatOptions format;
    for (const auto& json : jsons) {
        Slice slice(json.data(), json.size());
        EXPECT_TRUE(serde.deserialize_one_cell_from_json(*values, slice, format).ok()) << json;
    }
    return values;
}

} // namespace

class VariantTypedPathIndexMergeTest : public IndexStorageTestFixture {
protected:
    // Other tests leave index compaction off.
    void SetUp() override {
        IndexStorageTestFixture::SetUp();
        _old_index_compaction = config::inverted_index_compaction_enable;
        config::inverted_index_compaction_enable = true;
    }
    void TearDown() override {
        config::inverted_index_compaction_enable = _old_index_compaction;
        IndexStorageTestFixture::TearDown();
    }
    bool _old_index_compaction = true;

    IndexTabletOptions options(int64_t tablet_id, const std::set<int64_t>& index_ids = {20001, 20002,
                                                                             20003}) {
        IndexTabletOptions options;
        options.tablet_id = tablet_id;
        options.index_storage_format = InvertedIndexStorageFormatPB::V3;
        VariantColumnSpec variant;
        variant.unique_id = kVariantUid;
        variant.name = "v";
        variant.max_subcolumns_count = 2048;
        for (const char* path : {"msg", "host", "code"}) {
            variant.predefined_paths.push_back(
                    VariantPathSpec {.path = path,
                                     .type = FieldType::OLAP_FIELD_TYPE_STRING,
                                     .nullable = true,
                                     .pattern_type = PatternTypePB::MATCH_NAME,
                                     .array_item_type = {},
                                     .array_item_nullable = true});
        }
        options.variant_columns = {std::move(variant)};
        auto msg = IndexSpec::field_pattern_index(20001, "idx_msg", kVariantUid, "msg");
        msg.properties["parser"] = "english";
        msg.properties["support_phrase"] = "true";
        auto host = IndexSpec::field_pattern_index(20002, "idx_host", kVariantUid, "host");
        host.properties["parser"] = "unicode";
        auto code = IndexSpec::field_pattern_index(20003, "idx_code", kVariantUid, "code");
        for (const auto& index : {msg, host, code}) {
            if (index_ids.contains(index.index_id)) {
                options.inverted_indexes.push_back(index);
            }
        }
        return options;
    }

    static IndexRowsetSpec rowset(int64_t version, int rows = 600) {
        std::mt19937_64 rng(77 + version);
        std::vector<std::string> jsons;
        for (int row = 0; row < rows; ++row) {
            std::string json = "{";
            if (row % 7 != 3) {
                json += R"("msg":"user u)" + std::to_string(rng() % 50) + " logged in from host " +
                        std::to_string(rng() % 9) + R"( after )" + std::to_string(rng() % 4) +
                        R"( retries",)";
            }
            json += R"("host":"web-)" + std::to_string(rng() % 20) + ".corp.example.com" + R"(",)";
            if (row % 5 != 0) {
                json += R"("code":"E)" + std::to_string(rng() % 30) + R"(",)";
            }
            json += R"("other":)" + std::to_string(row) + "}";
            jsons.push_back(std::move(json));
        }
        IndexRowsetSpec spec;
        spec.version = version;
        spec.max_rows_per_segment = 100000;
        spec.batches.push_back(IndexBatch::single_variant(std::move(jsons),
                                                          static_cast<int32_t>(version) * 1000));
        return spec;
    }

    // The compaction of three written rowsets, and the CPU seconds the compaction took.
    RowsetSharedPtr write_and_compact(int64_t tablet_id,
                                      const std::set<int64_t>& index_ids = {20001, 20002, 20003},
                                      int rows = 600, double* compaction_cpu = nullptr) {
        EXPECT_TRUE(create_tablet(options(tablet_id, index_ids)).ok());
        std::vector<RowsetSharedPtr> rowsets;
        for (int64_t version = 0; version < 3; ++version) {
            auto written = write_rowset(rowset(version, rows));
            EXPECT_TRUE(written.has_value()) << written.error();
            if (!written.has_value()) {
                return nullptr;
            }
            rowsets.push_back(written.value());
        }
        const double cpu_before = process_cpu_seconds();
        auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets);
        if (compaction_cpu != nullptr) {
            *compaction_cpu = process_cpu_seconds() - cpu_before;
        }
        EXPECT_TRUE(compacted.has_value()) << compacted.error();
        return compacted.has_value() ? compacted.value() : nullptr;
    }

    static double process_cpu_seconds() {
        timespec ts {};
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts);
        return static_cast<double>(ts.tv_sec) + static_cast<double>(ts.tv_nsec) / 1e9;
    }

    static InvertedIndexDirectoryMap directories(const RowsetSharedPtr& rowset,
                                                 std::shared_ptr<IndexFileReader>* reader,
                                                 int segment = 0) {
        auto seg_path = rowset->segment_path(segment);
        EXPECT_TRUE(seg_path.has_value());
        *reader = std::make_shared<IndexFileReader>(
                rowset->rowset_meta()->fs(),
                std::string(InvertedIndexDescriptor::get_index_file_path_prefix(seg_path.value())),
                InvertedIndexStorageFormatPB::V3);
        EXPECT_TRUE((*reader)->init().ok());
        auto dirs = (*reader)->get_all_directories();
        EXPECT_TRUE(dirs.has_value());
        return dirs.has_value() ? dirs.value() : InvertedIndexDirectoryMap {};
    }
};

// With enable_variant_typed_path_index_compaction, a cumulative compaction merges the indexes of
// typed VARIANT paths instead of rebuilding them, and the merged indexes hold the same terms,
// documents, frequencies and positions as the rebuilt ones.
TEST_F(VariantTypedPathIndexMergeTest, MergedTypedPathIndexesEqualRebuiltOnes) {
    const bool old_flag = config::enable_variant_typed_path_index_compaction;
    config::enable_variant_typed_path_index_compaction = false;
    auto rebuilt = write_and_compact(130001);
    ASSERT_NE(rebuilt, nullptr);

    config::enable_variant_typed_path_index_compaction = true;
    const int64_t merged_before = exposed("compaction_variant_index_merge_paths");
    const int64_t rebuilt_before = exposed("compaction_variant_index_rebuild_paths");
    const int64_t aborted_before = exposed("compaction_variant_index_scan_aborted");
    auto merged = write_and_compact(130002);
    config::enable_variant_typed_path_index_compaction = old_flag;
    ASSERT_NE(merged, nullptr);
    EXPECT_EQ(exposed("compaction_variant_index_merge_paths") - merged_before, 3);
    EXPECT_EQ(exposed("compaction_variant_index_rebuild_paths") - rebuilt_before, 0);
    EXPECT_EQ(exposed("compaction_variant_index_scan_aborted") - aborted_before, 0);
    EXPECT_EQ(merged->num_rows(), rebuilt->num_rows());

    std::shared_ptr<IndexFileReader> rebuilt_reader;
    std::shared_ptr<IndexFileReader> merged_reader;
    auto rebuilt_dirs = directories(rebuilt, &rebuilt_reader);
    auto merged_dirs = directories(merged, &merged_reader);
    ASSERT_EQ(rebuilt_dirs.size(), merged_dirs.size());
    size_t compared = 0;
    for (const auto& [key, dir] : rebuilt_dirs) {
        auto it = merged_dirs.find(key);
        ASSERT_NE(it, merged_dirs.end()) << key.first << " " << key.second;
        auto expected = postings(dir.get());
        auto actual = postings(it->second.get());
        ASSERT_FALSE(expected.empty()) << key.first << " " << key.second;
        EXPECT_TRUE(expected == actual) << key.first << " " << key.second << ": "
                                        << expected.size() << " vs " << actual.size() << " terms";
        ++compared;
    }
    EXPECT_EQ(compared, 3);
}

// VARIANT V2 inputs where an input segment lacks a typed path entirely, where a path is all
// null, and where a path has values in only a few rows: the merged indexes equal the rebuilt
// ones, null documents included, across several destination segments.
TEST_F(VariantTypedPathIndexMergeTest, MergeHandlesAbsentAllNullAndSparseV2Paths) {
    const bool old_flag = config::enable_variant_typed_path_index_compaction;
    const int64_t old_segment_size = config::vertical_compaction_max_segment_size;
    config::vertical_compaction_max_segment_size = 8 * 1024;
    Defer restore {[&]() {
        config::enable_variant_typed_path_index_compaction = old_flag;
        config::vertical_compaction_max_segment_size = old_segment_size;
    }};
    auto write_and_compact_v2 = [&](int64_t tablet_id) -> RowsetSharedPtr {
        EXPECT_TRUE(create_tablet(options(tablet_id)).ok());
        std::vector<RowsetSharedPtr> rowsets;
        for (int64_t version = 0; version < 3; ++version) {
            std::mt19937_64 rng(900 + version);
            std::vector<std::string> jsons;
            for (int row = 0; row < 1500; ++row) {
                std::string json = R"({"msg":"user u)" + std::to_string(rng() % 50) +
                                   " logged in from host " + std::to_string(rng() % 9) + R"(",)";
                if (version == 2) {
                    json += R"("host":null,)"; // all null
                } else {
                    json += R"("host":"web-)" + std::to_string(rng() % 20) + R"(.corp",)";
                }
                if (version == 0 || (version == 2 && row % 500 == 7)) {
                    // version 1 has no code at all; version 2 has it in three rows
                    json += R"("code":"E)" + std::to_string(rng() % 30) + R"(",)";
                }
                json += R"("other":)" + std::to_string(row) + "}";
                jsons.push_back(std::move(json));
            }
            IndexRowsetSpec spec;
            spec.version = version;
            spec.max_rows_per_segment = 100000;
            spec.batches.push_back(IndexBatch::single_variant_column(
                    variant_v2_column(jsons), static_cast<int32_t>(version) * 2000));
            auto written = write_rowset(spec);
            EXPECT_TRUE(written.has_value()) << written.error();
            if (!written.has_value()) {
                return nullptr;
            }
            rowsets.push_back(written.value());
        }
        auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets);
        EXPECT_TRUE(compacted.has_value()) << compacted.error();
        return compacted.has_value() ? compacted.value() : nullptr;
    };

    config::enable_variant_typed_path_index_compaction = false;
    auto rebuilt = write_and_compact_v2(130011);
    ASSERT_NE(rebuilt, nullptr);
    config::enable_variant_typed_path_index_compaction = true;
    const int64_t merged_before = exposed("compaction_variant_index_merge_paths");
    const int64_t missing_before = exposed("compaction_variant_index_rebuild_missing_in_segment");
    auto merged = write_and_compact_v2(130012);
    ASSERT_NE(merged, nullptr);
    EXPECT_EQ(exposed("compaction_variant_index_merge_paths") - merged_before, 3);
    EXPECT_EQ(exposed("compaction_variant_index_rebuild_missing_in_segment") - missing_before, 0);
    ASSERT_EQ(merged->num_rows(), rebuilt->num_rows());
    ASSERT_EQ(merged->num_segments(), rebuilt->num_segments());
    EXPECT_GT(merged->num_segments(), 1);

    size_t compared = 0;
    for (int segment = 0; segment < merged->num_segments(); ++segment) {
        std::shared_ptr<IndexFileReader> rebuilt_reader;
        std::shared_ptr<IndexFileReader> merged_reader;
        auto rebuilt_dirs = directories(rebuilt, &rebuilt_reader, segment);
        auto merged_dirs = directories(merged, &merged_reader, segment);
        ASSERT_EQ(rebuilt_dirs.size(), merged_dirs.size()) << "segment " << segment;
        for (const auto& [key, dir] : rebuilt_dirs) {
            auto it = merged_dirs.find(key);
            ASSERT_NE(it, merged_dirs.end()) << key.first << " " << key.second;
            EXPECT_TRUE(postings(dir.get()) == postings(it->second.get()))
                    << "segment " << segment << " " << key.second;
            EXPECT_TRUE(null_bitmap(dir.get()) == null_bitmap(it->second.get()))
                    << "segment " << segment << " " << key.second << ": "
                    << null_bitmap(dir.get()).cardinality() << " vs "
                    << null_bitmap(it->second.get()).cardinality() << " nulls";
            ++compared;
        }
    }
    EXPECT_GE(compared, 6);
}

// Typed paths shaped like melian.logs fields, one index per run.
struct BenchKind {
    std::string name;
    std::string path;
    std::map<std::string, std::string> properties;
};

class VariantTypedPathIndexMergeBench : public VariantTypedPathIndexMergeTest {
protected:
    static constexpr const char* kPaths[] = {"message", "cmd_line", "file_path", "hostname", "uid"};

    IndexTabletOptions bench_options(int64_t tablet_id, const BenchKind& kind) {
        IndexTabletOptions options;
        options.tablet_id = tablet_id;
        options.index_storage_format = InvertedIndexStorageFormatPB::V3;
        VariantColumnSpec variant;
        variant.unique_id = kVariantUid;
        variant.name = "v";
        variant.max_subcolumns_count = 2048;
        for (const char* path : kPaths) {
            variant.predefined_paths.push_back(
                    VariantPathSpec {.path = path,
                                     .type = FieldType::OLAP_FIELD_TYPE_STRING,
                                     .nullable = true,
                                     .pattern_type = PatternTypePB::MATCH_NAME,
                                     .array_item_type = {},
                                     .array_item_nullable = true});
        }
        options.variant_columns = {std::move(variant)};
        if (!kind.path.empty()) {
            auto index = IndexSpec::field_pattern_index(21001, "idx_" + kind.path, kVariantUid,
                                                        kind.path);
            index.properties.insert(kind.properties.begin(), kind.properties.end());
            options.inverted_indexes = {index};
        }
        return options;
    }

    static std::string word(std::mt19937_64& rng, size_t vocabulary) {
        static const char* syllables[] = {"ka", "lo", "mi", "ten", "ra", "vex", "sul", "po",
                                          "dri", "an", "qu", "zor", "el", "ny", "fa", "gim"};
        uint64_t id = rng() % vocabulary;
        std::string w;
        do {
            w += syllables[id % 16];
            id /= 16;
        } while (id != 0);
        return w;
    }

    static std::string hex(std::mt19937_64& rng, int n) {
        static const char digits[] = "0123456789abcdef";
        std::string out;
        for (int i = 0; i < n; ++i) {
            out += digits[rng() % 16];
        }
        return out;
    }

    static IndexRowsetSpec bench_rowset(int64_t version, int rows) {
        std::mt19937_64 rng(1000 + version);
        std::vector<std::string> jsons;
        jsons.reserve(rows);
        for (int row = 0; row < rows; ++row) {
            std::string message = "User " + word(rng, 5000) + " " + word(rng, 300) + " session " +
                                  std::to_string(rng() % 100000) + " from 10." +
                                  std::to_string(rng() % 256) + "." + std::to_string(rng() % 256) +
                                  " status=" + word(rng, 40) + " " + word(rng, 2000) + " " +
                                  word(rng, 2000) + " " + word(rng, 20000);
            std::string cmd = "C:/Windows/System32/" + word(rng, 200) +
                              ".exe -NoProfile -ExecutionPolicy Bypass -File C:/Users/" +
                              word(rng, 3000) + "/AppData/Local/Temp/" + hex(rng, 12) +
                              ".ps1 -Arg " + hex(rng, 24);
            std::string path = "/var/lib/" + word(rng, 100) + "/" + word(rng, 2000) + "/" +
                               hex(rng, 8) + "." + word(rng, 10);
            std::string host = word(rng, 50) + "-" + word(rng, 4000) + "-" +
                               std::to_string(rng() % 100) + ".corp";
            std::string json = R"({"message":")" + message + R"(","cmd_line":")" + cmd +
                               R"(","file_path":")" + path + R"(","hostname":")" + host +
                               R"(","uid":")" + hex(rng, 32) + R"(","n":)" +
                               std::to_string(row) + "}";
            jsons.push_back(std::move(json));
        }
        IndexRowsetSpec spec;
        spec.version = version;
        spec.max_rows_per_segment = 10000000;
        spec.batches.push_back(IndexBatch::single_variant(std::move(jsons),
                                                          static_cast<int32_t>(version) * rows));
        return spec;
    }

    RowsetSharedPtr bench_write_and_compact(int64_t tablet_id, const BenchKind& kind, int rows,
                                            double* compaction_cpu) {
        EXPECT_TRUE(create_tablet(bench_options(tablet_id, kind)).ok());
        std::vector<RowsetSharedPtr> rowsets;
        for (int64_t version = 0; version < 3; ++version) {
            auto written = write_rowset(bench_rowset(version, rows));
            EXPECT_TRUE(written.has_value()) << written.error();
            if (!written.has_value()) {
                return nullptr;
            }
            rowsets.push_back(written.value());
        }
        const double cpu_before = process_cpu_seconds();
        auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets);
        *compaction_cpu = process_cpu_seconds() - cpu_before;
        EXPECT_TRUE(compacted.has_value()) << compacted.error();
        return compacted.has_value() ? compacted.value() : nullptr;
    }
};

// An input rowset marked to skip index compaction of the VARIANT makes every typed path rebuild,
// counted under its reason.
TEST_F(VariantTypedPathIndexMergeTest, RejectedPathsAreCountedByReason) {
    const bool old_flag = config::enable_variant_typed_path_index_compaction;
    config::enable_variant_typed_path_index_compaction = true;
    ASSERT_TRUE(create_tablet(options(130003)).ok());
    std::vector<RowsetSharedPtr> rowsets;
    for (int64_t version = 0; version < 3; ++version) {
        auto written = write_rowset(rowset(version));
        ASSERT_TRUE(written.has_value()) << written.error();
        rowsets.push_back(written.value());
    }
    static_cast<BetaRowset*>(rowsets[1].get())->set_skip_index_compaction(kVariantUid);
    const int64_t merged_before = exposed("compaction_variant_index_merge_paths");
    const int64_t rebuilt_before = exposed("compaction_variant_index_rebuild_paths");
    const int64_t skip_before = exposed("compaction_variant_index_rebuild_skip_marked");
    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets);
    config::enable_variant_typed_path_index_compaction = old_flag;
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    EXPECT_EQ(exposed("compaction_variant_index_merge_paths") - merged_before, 0);
    EXPECT_EQ(exposed("compaction_variant_index_rebuild_paths") - rebuilt_before, 3);
    EXPECT_EQ(exposed("compaction_variant_index_rebuild_skip_marked") - skip_before, 3);
}

// Compaction CPU with each index kind merged and rebuilt. Runs only when
// VARIANT_INDEX_MERGE_BENCH_ROWS gives the rows per input rowset.
TEST_F(VariantTypedPathIndexMergeBench, MergeVersusRebuildCpu) {
    const char* rows_env = std::getenv("VARIANT_INDEX_MERGE_BENCH_ROWS");
    if (rows_env == nullptr) {
        GTEST_SKIP() << "set VARIANT_INDEX_MERGE_BENCH_ROWS to run";
    }
    const int rows = std::atoi(rows_env);
    ScopedAnalyzers analyzers;
    // Ordered inputs with large segments would be linked rather than merged.
    const bool old_ordered = config::enable_ordered_data_compaction;
    config::enable_ordered_data_compaction = false;
    const std::map<std::string, std::string> ngram_phrase {{"analyzer", "ngram4_lower"},
                                                           {"support_phrase", "true"}};
    const std::vector<BenchKind> kinds {
            {"no index", "", {}},
            {"alnum_lower+phrase", "message", {{"analyzer", "alnum_lower"}, {"support_phrase", "true"}}},
            {"ngram4_lower+phrase cmd", "cmd_line", ngram_phrase},
            {"ngram4_lower+phrase path", "file_path", ngram_phrase},
            {"ngram4_lower+phrase name", "hostname", ngram_phrase},
            {"eq high-card", "uid", {}},
    };
    const bool old_flag = config::enable_variant_typed_path_index_compaction;
    int64_t tablet_id = 132000;
    for (const auto& kind : kinds) {
        for (bool merge : {false, true, false, true}) {
            config::enable_variant_typed_path_index_compaction = merge;
            double cpu = 0;
            const int64_t merged_before = exposed("compaction_variant_index_merge_paths");
            auto compacted = bench_write_and_compact(++tablet_id, kind, rows, &cpu);
            ASSERT_NE(compacted, nullptr);
            std::cout << "MERGE_BENCH kind=" << kind.name << " mode="
                      << (merge ? "merge" : "rebuild") << " merged_paths="
                      << exposed("compaction_variant_index_merge_paths") - merged_before
                      << " rows=" << compacted->num_rows() << " compaction_cpu_s=" << cpu
                      << std::endl;
        }
    }
    config::enable_variant_typed_path_index_compaction = old_flag;
    config::enable_ordered_data_compaction = old_ordered;
}

} // namespace doris::index_storage_test
