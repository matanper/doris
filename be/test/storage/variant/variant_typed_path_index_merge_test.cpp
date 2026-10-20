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

#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "common/config.h"
#include "storage/index/index_file_reader.h"
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

} // namespace

class VariantTypedPathIndexMergeTest : public IndexStorageTestFixture {
protected:
    IndexTabletOptions options(int64_t tablet_id) {
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
        options.inverted_indexes = {msg, host, code};
        return options;
    }

    static IndexRowsetSpec rowset(int64_t version) {
        std::mt19937_64 rng(77 + version);
        std::vector<std::string> jsons;
        for (int row = 0; row < 600; ++row) {
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

    RowsetSharedPtr write_and_compact(int64_t tablet_id) {
        EXPECT_TRUE(create_tablet(options(tablet_id)).ok());
        std::vector<RowsetSharedPtr> rowsets;
        for (int64_t version = 0; version < 3; ++version) {
            auto written = write_rowset(rowset(version));
            EXPECT_TRUE(written.has_value()) << written.error();
            if (!written.has_value()) {
                return nullptr;
            }
            rowsets.push_back(written.value());
        }
        auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets);
        EXPECT_TRUE(compacted.has_value()) << compacted.error();
        return compacted.has_value() ? compacted.value() : nullptr;
    }

    static InvertedIndexDirectoryMap directories(const RowsetSharedPtr& rowset,
                                                 std::shared_ptr<IndexFileReader>* reader) {
        auto seg_path = rowset->segment_path(0);
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
    auto merged = write_and_compact(130002);
    config::enable_variant_typed_path_index_compaction = old_flag;
    ASSERT_NE(merged, nullptr);
    EXPECT_EQ(exposed("compaction_variant_index_merge_paths") - merged_before, 3);
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

} // namespace doris::index_storage_test
