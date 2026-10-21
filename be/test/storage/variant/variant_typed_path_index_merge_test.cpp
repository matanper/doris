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
#include <bvar/bvar.h>
#include <gtest/gtest.h>
#include <time.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <map>
#include <random>
#include <roaring/roaring.hh>
#include <set>
#include <string>
#include <vector>

#include "common/config.h"
#include "core/column/column_nullable.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "cpp/sync_point.h"
#include "io/fs/file_system.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/variant/index_storage_variant_test_base.h"
#include "util/defer_op.h"

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

    IndexTabletOptions options(int64_t tablet_id,
                               const std::set<int64_t>& index_ids = {20001, 20002, 20003}) {
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

    static constexpr int kDeletedRows = 100;

    // With deleted_row_terms, the first kDeletedRows rows of version 1 carry terms that no other
    // row has, so a delete of exactly those rows leaves terms whose every posting is dropped.
    static IndexRowsetSpec rowset(int64_t version, int rows = 600, bool deleted_row_terms = false) {
        std::mt19937_64 rng(77 + version);
        std::vector<std::string> jsons;
        for (int row = 0; row < rows; ++row) {
            if (deleted_row_terms && version == 1 && row < kDeletedRows) {
                const std::string n = std::to_string(row);
                jsons.push_back(R"({"msg":"purgedterm)" + n + R"( logged in","host":"doomed-)" + n +
                                R"(.corp.example.com","code":"ZGONE)" + n + R"(","other":)" + n +
                                "}");
                continue;
            }
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
        spec.batches.push_back(
                IndexBatch::single_variant(std::move(jsons), static_cast<int32_t>(version) * 1000));
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

    // A full compaction of three written rowsets plus a fourth whose delete predicate removes
    // keys [1000, 1000 + kDeletedRows), the rows of version 1 that alone hold the
    // deleted_row_terms. *source receives the version 1 rowset.
    RowsetSharedPtr write_delete_and_compact(int64_t tablet_id, RowsetSharedPtr* source) {
        EXPECT_TRUE(create_tablet(options(tablet_id)).ok());
        std::vector<RowsetSharedPtr> rowsets;
        for (int64_t version = 0; version < 3; ++version) {
            auto written = write_rowset(rowset(version, 600, true));
            EXPECT_TRUE(written.has_value()) << written.error();
            if (!written.has_value()) {
                return nullptr;
            }
            rowsets.push_back(written.value());
        }
        *source = rowsets[1];

        IndexRowsetSpec spec;
        spec.version = 3;
        spec.max_rows_per_segment = 100000;
        spec.batches.push_back(IndexBatch::single_variant(
                {R"({"msg":"user u1 logged in from host 1 after 1 retries",)"
                 R"("host":"web-1.corp.example.com","code":"E1","other":0})"},
                3000));
        auto written = write_rowset(spec);
        EXPECT_TRUE(written.has_value()) << written.error();
        if (!written.has_value()) {
            return nullptr;
        }
        DeletePredicatePB predicate;
        predicate.set_version(3);
        for (const auto& [op, value] : std::vector<std::pair<std::string, std::string>> {
                     {">=", "1000"}, {"<<", std::to_string(1000 + kDeletedRows)}}) {
            auto* sub = predicate.add_sub_predicates_v2();
            sub->set_column_unique_id(1);
            sub->set_column_name("k");
            sub->set_op(op);
            sub->set_cond_value(value);
        }
        written.value()->rowset_meta()->set_delete_predicate(std::move(predicate));
        rowsets.push_back(written.value());

        auto compacted = compact_rowsets(IndexCompactionKind::FULL, rowsets);
        EXPECT_TRUE(compacted.has_value()) << compacted.error();
        return compacted.has_value() ? compacted.value() : nullptr;
    }

    // A rowset of `version` without any segment, added to the tablet, as a version hole is.
    RowsetSharedPtr write_empty_rowset(int64_t version) {
        RowsetWriterContext context;
        RowsetId rowset_id;
        rowset_id.init(version + 1000);
        context.rowset_id = rowset_id;
        context.rowset_type = BETA_ROWSET;
        context.data_dir = data_dir();
        context.rowset_state = VISIBLE;
        context.tablet_schema = tablet_schema();
        context.tablet_path = tablet()->tablet_path();
        context.tablet_id = tablet()->tablet_id();
        context.tablet_uid = tablet()->tablet_uid();
        context.tablet = tablet();
        context.version = Version(version, version);
        context.segments_overlap = NONOVERLAPPING;
        auto writer = RowsetFactory::create_rowset_writer(*storage_engine(), context, false);
        EXPECT_TRUE(writer.has_value()) << writer.error();
        if (!writer.has_value()) {
            return nullptr;
        }
        RowsetSharedPtr rowset;
        EXPECT_TRUE(writer.value()->build(rowset).ok());
        EXPECT_TRUE(tablet()->add_rowset(rowset).ok());
        return rowset;
    }

    // Three written rowsets and, with `empty_last`, a fourth without segments.
    std::vector<RowsetSharedPtr> write_inputs(int64_t tablet_id, bool empty_last) {
        EXPECT_TRUE(create_tablet(options(tablet_id)).ok());
        std::vector<RowsetSharedPtr> rowsets;
        for (int64_t version = 0; version < 3; ++version) {
            auto written = write_rowset(rowset(version));
            EXPECT_TRUE(written.has_value()) << written.error();
            if (!written.has_value()) {
                return {};
            }
            rowsets.push_back(written.value());
        }
        if (empty_last) {
            rowsets.push_back(write_empty_rowset(3));
        }
        return rowsets;
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
        EXPECT_TRUE(expected == actual) << key.first << " " << key.second << ": " << expected.size()
                                        << " vs " << actual.size() << " terms";
        ++compared;
    }
    EXPECT_EQ(compared, 3);
}

// Rows removed by a delete predicate map to no destination row, so terms that only those rows
// held have no documents left. The merged indexes drop them exactly as a rebuild does: the
// complete term maps are equal and none of those terms remain.
TEST_F(VariantTypedPathIndexMergeTest, MergedTypedPathIndexesEqualRebuiltOnesWithDeletedRows) {
    const bool old_flag = config::enable_variant_typed_path_index_compaction;
    Defer restore {[&] { config::enable_variant_typed_path_index_compaction = old_flag; }};
    const auto deleted_only = [](const std::string& term) {
        return term.find("purgedterm") != std::string::npos ||
               term.find("doomed") != std::string::npos || term.find("ZGONE") != std::string::npos;
    };

    config::enable_variant_typed_path_index_compaction = false;
    RowsetSharedPtr rebuilt_source;
    auto rebuilt = write_delete_and_compact(130011, &rebuilt_source);
    ASSERT_NE(rebuilt, nullptr);

    config::enable_variant_typed_path_index_compaction = true;
    const int64_t merged_before = exposed("compaction_variant_index_merge_paths");
    const int64_t rebuilt_before = exposed("compaction_variant_index_rebuild_paths");
    RowsetSharedPtr merged_source;
    auto merged = write_delete_and_compact(130012, &merged_source);
    ASSERT_NE(merged, nullptr);
    EXPECT_EQ(exposed("compaction_variant_index_merge_paths") - merged_before, 3);
    EXPECT_EQ(exposed("compaction_variant_index_rebuild_paths") - rebuilt_before, 0);
    EXPECT_EQ(rebuilt->num_rows(), 3 * 600 + 1 - kDeletedRows);
    EXPECT_EQ(merged->num_rows(), rebuilt->num_rows());

    // The source index does hold the deleted-only terms, in each of the three paths.
    std::shared_ptr<IndexFileReader> source_reader;
    size_t source_paths_with_deleted_terms = 0;
    for (const auto& [key, dir] : directories(merged_source, &source_reader)) {
        auto terms = postings(dir.get());
        if (std::any_of(terms.begin(), terms.end(),
                        [&](const auto& entry) { return deleted_only(entry.first); })) {
            ++source_paths_with_deleted_terms;
        }
    }
    EXPECT_EQ(source_paths_with_deleted_terms, 3);

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
        EXPECT_TRUE(expected == actual) << key.first << " " << key.second << ": " << expected.size()
                                        << " vs " << actual.size() << " terms";
        for (const auto& [term, docs] : actual) {
            EXPECT_FALSE(deleted_only(term)) << key.first << " keeps " << term;
            EXPECT_FALSE(docs.empty()) << key.first << " keeps " << term << " with no documents";
        }
        ++compared;
    }
    EXPECT_EQ(compared, 3);
}

// An input rowset without segments, whose meta has no storage resource as a cloud version hole's
// may not, is skipped: the typed paths still merge and the scan is not aborted.
TEST_F(VariantTypedPathIndexMergeTest, InputsWithoutSegmentsOrStorageStillMerge) {
    const bool old_flag = config::enable_variant_typed_path_index_compaction;
    config::enable_variant_typed_path_index_compaction = true;
    auto rowsets = write_inputs(130301, true);
    ASSERT_EQ(rowsets.size(), 4);
    ASSERT_NE(rowsets[3], nullptr);
    ASSERT_EQ(rowsets[3]->num_segments(), 0);

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    bool empty_rowset_resolved = false;
    SyncPoint::CallbackGuard fs_guard;
    sp->set_call_back(
            "Compaction::construct_variant_index_compaction_paths:fs",
            [&](auto&& args) {
                auto* rowset = try_any_cast<BetaRowset*>(args[0]);
                if (rowset->num_segments() == 0) {
                    empty_rowset_resolved = true;
                    *try_any_cast<io::FileSystemSPtr*>(args[1]) = nullptr;
                }
            },
            &fs_guard);
    Defer restore {[&]() {
        sp->disable_processing();
        config::enable_variant_typed_path_index_compaction = old_flag;
    }};

    const int64_t merged_before = exposed("compaction_variant_index_merge_paths");
    const int64_t aborted_before = exposed("compaction_variant_index_scan_aborted");
    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets);
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    EXPECT_EQ(compacted.value()->num_rows(), 3 * 600);
    EXPECT_FALSE(empty_rowset_resolved);
    EXPECT_EQ(exposed("compaction_variant_index_merge_paths") - merged_before, 3);
    EXPECT_EQ(exposed("compaction_variant_index_scan_aborted") - aborted_before, 0);
}

// An input rowset with segments whose storage cannot be resolved stops the scan: every typed path
// of the compaction is rebuilt, and the abort is counted once.
TEST_F(VariantTypedPathIndexMergeTest, AnAbortedScanIsCountedOnce) {
    const bool old_flag = config::enable_variant_typed_path_index_compaction;
    config::enable_variant_typed_path_index_compaction = true;
    auto rowsets = write_inputs(130302, false);
    ASSERT_EQ(rowsets.size(), 3);

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    SyncPoint::CallbackGuard fs_guard;
    sp->set_call_back(
            "Compaction::construct_variant_index_compaction_paths:fs",
            [&](auto&& args) {
                if (try_any_cast<BetaRowset*>(args[0]) == rowsets[1].get()) {
                    *try_any_cast<io::FileSystemSPtr*>(args[1]) = nullptr;
                }
            },
            &fs_guard);
    Defer restore {[&]() {
        sp->disable_processing();
        config::enable_variant_typed_path_index_compaction = old_flag;
    }};

    const int64_t merged_before = exposed("compaction_variant_index_merge_paths");
    const int64_t rebuilt_before = exposed("compaction_variant_index_rebuild_paths");
    const int64_t aborted_before = exposed("compaction_variant_index_scan_aborted");
    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets);
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    EXPECT_EQ(compacted.value()->num_rows(), 3 * 600);
    EXPECT_EQ(exposed("compaction_variant_index_scan_aborted") - aborted_before, 1);
    EXPECT_EQ(exposed("compaction_variant_index_merge_paths") - merged_before, 0);
    EXPECT_EQ(exposed("compaction_variant_index_rebuild_paths") - rebuilt_before, 0);
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

// Index properties are compared as the schema defines the index, by what decides its content:
// per-subcolumn copies of an index are ignored, defaults equal missing keys, and a real
// difference or an index known only through copies rebuilds. (A rowset in another index storage
// format cannot be read by a compaction at all, so its rejection is not exercised here.)
TEST_F(VariantTypedPathIndexMergeTest, IndexPropertiesComparedByContent) {
    struct Case {
        const char* name;
        std::function<void(TabletSchemaPB&)> patch;
        int64_t merged;
        int64_t props_mismatch;
        int64_t format_mismatch;
    };
    auto reorder = [](TabletSchemaPB& pb,
                      const std::function<void(std::vector<TabletIndexPB>&)>& fn) {
        std::vector<TabletIndexPB> indexes(pb.index().begin(), pb.index().end());
        fn(indexes);
        pb.clear_index();
        for (auto& index : indexes) {
            *pb.add_index() = index;
        }
    };
    auto stripped_copy = [](const TabletIndexPB& index) {
        TabletIndexPB copy = index;
        copy.set_index_suffix_name("v%2Eother");
        copy.mutable_properties()->erase("parser");
        copy.mutable_properties()->erase("support_phrase");
        return copy;
    };
    const std::vector<Case> cases {
            {"stripped copy listed first",
             [&](TabletSchemaPB& pb) {
                 reorder(pb, [&](std::vector<TabletIndexPB>& indexes) {
                     for (const auto& index : std::vector<TabletIndexPB>(indexes)) {
                         if (index.index_id() == 20001) {
                             indexes.insert(indexes.begin(), stripped_copy(index));
                         }
                     }
                 });
             },
             3, 0, 0},
            {"explicit default",
             [](TabletSchemaPB& pb) {
                 for (auto& index : *pb.mutable_index()) {
                     if (index.index_id() == 20001) {
                         (*index.mutable_properties())["lower_case"] = "true";
                     }
                 }
             },
             3, 0, 0},
            {"different parser",
             [](TabletSchemaPB& pb) {
                 for (auto& index : *pb.mutable_index()) {
                     if (index.index_id() == 20001) {
                         (*index.mutable_properties())["parser"] = "unicode";
                     }
                 }
             },
             2, 1, 0},
            // idx_code has no parser: its lower_case changes neither the terms it writes nor
            // the terms its reader looks up.
            {"lower_case false without a parser",
             [](TabletSchemaPB& pb) {
                 for (auto& index : *pb.mutable_index()) {
                     if (index.index_id() == 20003) {
                         (*index.mutable_properties())["lower_case"] = "false";
                     }
                 }
             },
             3, 0, 0},
            // idx_msg has the english parser: lower_case decides its terms.
            {"lower_case false with a parser",
             [](TabletSchemaPB& pb) {
                 for (auto& index : *pb.mutable_index()) {
                     if (index.index_id() == 20001) {
                         (*index.mutable_properties())["lower_case"] = "false";
                     }
                 }
             },
             2, 1, 0},
            {"only copies",
             [&](TabletSchemaPB& pb) {
                 reorder(pb, [&](std::vector<TabletIndexPB>& indexes) {
                     for (auto& index : indexes) {
                         if (index.index_id() == 20001) {
                             index.set_index_suffix_name("v%2Eother");
                         }
                     }
                 });
             },
             2, 1, 0},
    };
    const bool old_flag = config::enable_variant_typed_path_index_compaction;
    config::enable_variant_typed_path_index_compaction = true;
    Defer restore {[&]() { config::enable_variant_typed_path_index_compaction = old_flag; }};
    int64_t tablet_id = 130100;
    for (const auto& c : cases) {
        ASSERT_TRUE(create_tablet(options(++tablet_id)).ok());
        std::vector<RowsetSharedPtr> rowsets;
        for (int64_t version = 0; version < 3; ++version) {
            auto written = write_rowset(rowset(version));
            ASSERT_TRUE(written.has_value()) << written.error();
            rowsets.push_back(written.value());
        }
        // The last input carries the patched schema; the output follows the first one.
        TabletSchemaPB pb;
        rowsets[2]->tablet_schema()->to_schema_pb(&pb);
        c.patch(pb);
        auto patched = std::make_shared<TabletSchema>();
        patched->init_from_pb(pb);
        rowsets[2]->rowset_meta()->set_tablet_schema(patched);
        rowsets[2]->_schema = patched;

        const int64_t merged_before = exposed("compaction_variant_index_merge_paths");
        const int64_t props_before = exposed("compaction_variant_index_rebuild_props_mismatch");
        const int64_t format_before =
                exposed("compaction_variant_index_rebuild_props_format_mismatch");
        auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets);
        ASSERT_TRUE(compacted.has_value()) << c.name << ": " << compacted.error();
        EXPECT_EQ(exposed("compaction_variant_index_merge_paths") - merged_before, c.merged)
                << c.name;
        EXPECT_EQ(exposed("compaction_variant_index_rebuild_props_mismatch") - props_before,
                  c.props_mismatch)
                << c.name;
        EXPECT_EQ(exposed("compaction_variant_index_rebuild_props_format_mismatch") - format_before,
                  c.format_mismatch)
                << c.name;
    }
}

// An input whose index properties differ from the output's only in ways the normalized
// comparison ignores still merges, and is counted once per compaction. Each distinct difference
// is logged once per process: a second compaction with the same difference logs nothing more.
TEST_F(VariantTypedPathIndexMergeTest, PropertiesEqualAfterNormalizeAreCountedAndLoggedOnce) {
    struct Case {
        const char* name;
        std::function<void(TabletSchemaPB&)> patch;
        int64_t logged_first;
    };
    const std::vector<Case> cases {
            // The defined entry of idx_host carries a key no content setting reads: raw_diff.
            {"extra key",
             [](TabletSchemaPB& pb) {
                 for (auto& index : *pb.mutable_index()) {
                     if (index.index_id() == 20002 && index.index_suffix_name().empty()) {
                         (*index.mutable_properties())["probe_note"] = "raw";
                     }
                 }
             },
             1},
            // A copy of idx_code with an extra key is listed first: the lookup by id alone would
            // compare the copy, matched_copy.
            {"copy listed first",
             [](TabletSchemaPB& pb) {
                 std::vector<TabletIndexPB> indexes(pb.index().begin(), pb.index().end());
                 for (const auto& index : std::vector<TabletIndexPB>(indexes)) {
                     if (index.index_id() == 20003 && index.index_suffix_name().empty()) {
                         TabletIndexPB copy = index;
                         copy.set_index_suffix_name("v%2Eprobe");
                         (*copy.mutable_properties())["probe_key"] = "probe_value";
                         indexes.insert(indexes.begin(), copy);
                     }
                 }
                 pb.clear_index();
                 for (auto& index : indexes) {
                     *pb.add_index() = index;
                 }
             },
             1},
    };
    const bool old_flag = config::enable_variant_typed_path_index_compaction;
    config::enable_variant_typed_path_index_compaction = true;
    Defer restore {[&]() { config::enable_variant_typed_path_index_compaction = old_flag; }};
    int64_t tablet_id = 130200;
    for (const auto& c : cases) {
        for (int run = 0; run < 2; ++run) {
            ASSERT_TRUE(create_tablet(options(++tablet_id)).ok());
            std::vector<RowsetSharedPtr> rowsets;
            for (int64_t version = 0; version < 3; ++version) {
                auto written = write_rowset(rowset(version));
                ASSERT_TRUE(written.has_value()) << written.error();
                rowsets.push_back(written.value());
            }
            TabletSchemaPB pb;
            rowsets[2]->tablet_schema()->to_schema_pb(&pb);
            c.patch(pb);
            auto patched = std::make_shared<TabletSchema>();
            patched->init_from_pb(pb);
            rowsets[2]->rowset_meta()->set_tablet_schema(patched);
            rowsets[2]->_schema = patched;

            const int64_t merged_before = exposed("compaction_variant_index_merge_paths");
            const int64_t equal_before =
                    exposed("compaction_variant_index_props_equal_after_normalize");
            const int64_t logged_before =
                    exposed("compaction_variant_index_props_normalize_logged");
            auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets);
            ASSERT_TRUE(compacted.has_value()) << c.name << ": " << compacted.error();
            EXPECT_EQ(exposed("compaction_variant_index_merge_paths") - merged_before, 3)
                    << c.name << " run " << run;
            EXPECT_EQ(
                    exposed("compaction_variant_index_props_equal_after_normalize") - equal_before,
                    1)
                    << c.name << " run " << run;
            EXPECT_EQ(exposed("compaction_variant_index_props_normalize_logged") - logged_before,
                      run == 0 ? c.logged_first : 0)
                    << c.name << " run " << run;
        }
    }
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
        static const char* syllables[] = {"ka",  "lo", "mi", "ten", "ra", "vex", "sul", "po",
                                          "dri", "an", "qu", "zor", "el", "ny",  "fa",  "gim"};
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
                               R"(","uid":")" + hex(rng, 32) + R"(","n":)" + std::to_string(row) +
                               "}";
            jsons.push_back(std::move(json));
        }
        IndexRowsetSpec spec;
        spec.version = version;
        spec.max_rows_per_segment = 10000000;
        spec.batches.push_back(
                IndexBatch::single_variant(std::move(jsons), static_cast<int32_t>(version) * rows));
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
            {"alnum_lower+phrase",
             "message",
             {{"analyzer", "alnum_lower"}, {"support_phrase", "true"}}},
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
            std::cout << "MERGE_BENCH kind=" << kind.name
                      << " mode=" << (merge ? "merge" : "rebuild") << " merged_paths="
                      << exposed("compaction_variant_index_merge_paths") - merged_before
                      << " rows=" << compacted->num_rows() << " compaction_cpu_s=" << cpu
                      << std::endl;
        }
    }
    config::enable_variant_typed_path_index_compaction = old_flag;
    config::enable_ordered_data_compaction = old_ordered;
}

} // namespace doris::index_storage_test
