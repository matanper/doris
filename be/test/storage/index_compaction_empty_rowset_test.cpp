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

#include <gtest/gtest.h>

#include <set>

#include "common/config.h"
#include "cpp/sync_point.h"
#include "io/fs/file_system.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/tablet/tablet.h"
#include "testutil/index_storage_test_util.h"
#include "util/defer_op.h"

namespace doris::index_storage_test {

class IndexCompactionEmptyRowsetTest : public IndexStorageTestFixture {
protected:
    // A rowset of `version` without any segment, added to the tablet.
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
};

// A source rowset without segments neither has its storage resolved nor takes the column out of
// index compaction: in cloud mode its meta may carry no resource at all.
TEST_F(IndexCompactionEmptyRowsetTest, EmptyRowsetKeepsTheColumnInIndexCompaction) {
    const bool saved = config::inverted_index_compaction_enable;
    config::inverted_index_compaction_enable = true;
    IndexTabletOptions options;
    options.tablet_id = 110101;
    options.text_columns = {TextColumnSpec {.unique_id = 2, .name = "title"}};
    options.inverted_indexes.push_back(IndexSpec::column_index(20001, "idx_title", 2));
    ASSERT_TRUE(create_tablet(options).ok());

    std::vector<RowsetSharedPtr> rowsets;
    for (int64_t version = 0; version < 3; ++version) {
        if (version == 1) {
            rowsets.push_back(write_empty_rowset(version));
            ASSERT_NE(rowsets.back(), nullptr);
            continue;
        }
        IndexRowsetSpec spec;
        spec.version = version;
        spec.batches.push_back(IndexBatch::single_text({"hello", "other"},
                                                       static_cast<int32_t>(version * 100)));
        auto written = write_rowset(spec);
        ASSERT_TRUE(written.has_value()) << written.error();
        rowsets.push_back(written.value());
    }
    ASSERT_EQ(rowsets[1]->num_segments(), 0);

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    bool empty_rowset_resolved = false;
    std::set<int32_t> index_compaction_columns;
    SyncPoint::CallbackGuard fs_guard;
    sp->set_call_back(
            "Compaction::check_rowset_has_inverted_index:fs",
            [&](auto&& args) {
                auto* rowset = try_any_cast<BetaRowset*>(args[0]);
                if (rowset->num_segments() == 0) {
                    empty_rowset_resolved = true;
                    // As for a cloud rowset meta without a resource.
                    *try_any_cast<io::FileSystemSPtr*>(args[1]) = nullptr;
                }
            },
            &fs_guard);
    SyncPoint::CallbackGuard columns_guard;
    sp->set_call_back(
            "Compaction::do_inverted_index_compaction:columns",
            [&](auto&& args) {
                index_compaction_columns = *try_any_cast<const std::set<int32_t>*>(args[0]);
            },
            &columns_guard);
    Defer restore {[&]() {
        sp->disable_processing();
        config::inverted_index_compaction_enable = saved;
    }};

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets);
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    EXPECT_EQ(compacted.value()->num_rows(), 4);
    EXPECT_FALSE(empty_rowset_resolved);
    EXPECT_EQ(index_compaction_columns, (std::set<int32_t> {2}));
}

} // namespace doris::index_storage_test
