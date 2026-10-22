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

#include <bvar/variable.h>
#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/row_cursor.h"
#include "storage/segment/column_meta_accessor.h"
#include "storage/segment/external_col_meta_util.h"
#include "storage/segment/row_ranges.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_iterator.h"
#include "storage/segment/test_segment_writer.h"
#include "storage/tablet/tablet_schema_helper.h"

namespace doris::segment_v2 {
namespace {

constexpr auto kTestDir = "./ut_dir/segment_iterator_expr_zonemap_test";
constexpr int kNumRows = 8192;
const RowsetId kRowsetId {.version = 1};

Field int_field(int32_t value) {
    return Field::create_field<TYPE_INT>(value);
}

class IntMaxAtLeastExpr final : public VExpr {
public:
    IntMaxAtLeastExpr(int column_id, int32_t threshold)
            : _column_id(column_id), _threshold(threshold) {
        _data_type = std::make_shared<DataTypeUInt8>();
    }

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column(VExprContext*, const Block*, Selector*, size_t,
                          ColumnPtr&) const override {
        return Status::InternalError("IntMaxAtLeastExpr is only used by zonemap tests");
    }

    bool can_evaluate_zonemap_filter() const override { return true; }

    bool is_constant() const override { return false; }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_column_id);
    }

    ZoneMapFilterResult evaluate_zonemap_filter(const ZoneMapEvalContext& ctx) const override {
        auto zone_map = ctx.zone_map(_column_id);
        if (zone_map == nullptr) {
            return unsupported_zonemap_filter(ctx);
        }
        if (!zone_map->has_not_null) {
            return ZoneMapFilterResult::kNoMatch;
        }
        return zone_map->max_value.get<TYPE_INT>() >= _threshold ? ZoneMapFilterResult::kMayMatch
                                                                 : ZoneMapFilterResult::kNoMatch;
    }

private:
    int _column_id;
    int32_t _threshold;
    std::string _expr_name = "int_max_at_least_expr";
};

TabletSchemaSPtr make_tablet_schema(
        TabletStorageFormatPB format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2) {
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->append_column(*doris::create_int_key(0, false));
    tablet_schema->append_column(*doris::create_int_key(1, false));
    tablet_schema->set_storage_page_size(4096);
    tablet_schema->set_storage_format(format);
    return tablet_schema;
}

// Answers whether a range is in the local file cache with a fixed value, and records the reads.
class LocalCacheProbeReader final : public io::FileReader {
public:
    LocalCacheProbeReader(io::FileReaderSPtr inner, bool in_local_cache)
            : _inner(std::move(inner)), _in_local_cache(in_local_cache) {}

    Status close() override { return _inner->close(); }
    const io::Path& path() const override { return _inner->path(); }
    size_t size() const override { return _inner->size(); }
    bool closed() const override { return _inner->closed(); }
    int64_t mtime() const override { return _inner->mtime(); }
    bool is_range_in_local_cache(size_t, size_t) override { return _in_local_cache; }

    size_t reads = 0;
    size_t bytes = 0;
    size_t largest_read = 0;

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
        ++reads;
        bytes += result.size;
        largest_read = std::max(largest_read, result.size);
        return _inner->read_at(offset, result, bytes_read, io_ctx);
    }

private:
    io::FileReaderSPtr _inner;
    bool _in_local_cache;
};

int64_t exposed_counter(const std::string& name) {
    return std::stoll(bvar::Variable::describe_exposed(name));
}

int64_t live_meta_regions() {
    return std::stoll(bvar::Variable::describe_exposed("segment_column_meta_regions"));
}

SchemaSPtr make_read_schema(const TabletSchemaSPtr& tablet_schema) {
    std::vector<ColumnId> read_column_ids(tablet_schema->num_columns());
    for (uint32_t cid = 0; cid < read_column_ids.size(); ++cid) {
        read_column_ids[cid] = cid;
    }
    return std::make_shared<Schema>(tablet_schema->columns(), read_column_ids);
}

} // namespace

class SegmentIteratorExprZonemapTest : public testing::Test {
protected:
    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        _tablet_schema = make_tablet_schema();
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    void build_segment(std::shared_ptr<Segment>* segment) {
        const auto path = std::string(kTestDir) + "/expr_zonemap_segment.dat";
        auto fs = io::global_local_filesystem();
        io::FileWriterPtr file_writer;
        auto st = fs->create_file(path, &file_writer);
        ASSERT_TRUE(st.ok()) << st;

        SegmentWriterOptions opts;
        opts.num_rows_per_block = 1024;
        TestSegmentWriter writer(file_writer.get(), 0, _tablet_schema, nullptr, nullptr, opts,
                                 nullptr);
        st = writer.init();
        ASSERT_TRUE(st.ok()) << st;

        RowCursor row;
        std::vector<Field> fields(_tablet_schema->num_columns(), Field(PrimitiveType::TYPE_NULL));
        st = row.init_scan_key(_tablet_schema, std::move(fields));
        ASSERT_TRUE(st.ok()) << st;
        for (int rid = 0; rid < kNumRows; ++rid) {
            row.mutable_field(0) = int_field(rid);
            row.mutable_field(1) = int_field(rid < kNumRows / 2 ? 0 : 1000);
            st = writer.append_row(row);
            ASSERT_TRUE(st.ok()) << st;
        }

        uint64_t file_size = 0;
        uint64_t index_size = 0;
        st = writer.finalize(&file_size, &index_size);
        ASSERT_TRUE(st.ok()) << st;
        st = file_writer->close();
        ASSERT_TRUE(st.ok()) << st;

        st = Segment::open(fs, path, 100, 0, kRowsetId, _tablet_schema, io::FileReaderOptions {},
                           segment);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(kNumRows, (*segment)->num_rows());
    }

    void prepare_expr_context(const VExprContextSPtr& expr_ctx) {
        RowDescriptor row_desc;
        auto st = expr_ctx->prepare(&_runtime_state, row_desc);
        ASSERT_TRUE(st.ok()) << st;
        st = expr_ctx->open(&_runtime_state);
        ASSERT_TRUE(st.ok()) << st;
    }

    TabletSchemaSPtr _tablet_schema;
    OlapReaderStatistics _stats;
    RuntimeState _runtime_state;
};

TEST_F(SegmentIteratorExprZonemapTest, NewIteratorPrunesWholeSegmentByExprZonemap) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    auto expr_ctx = std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(1, 2000));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_ctx));
    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.common_expr_ctxs_push_down = {expr_ctx};

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    EXPECT_TRUE(iter->empty());
    EXPECT_EQ(1, _stats.total_segment_number);
    EXPECT_EQ(1, _stats.filtered_segment_number);
    EXPECT_EQ(1, _stats.expr_zonemap_filtered_segments);
}

TEST_F(SegmentIteratorExprZonemapTest, NewIteratorKeepsSegmentWhenExprZonemapMayMatch) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    auto expr_ctx = std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(1, 500));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_ctx));
    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.common_expr_ctxs_push_down = {expr_ctx};

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    EXPECT_FALSE(iter->empty());
    EXPECT_EQ(1, _stats.total_segment_number);
    EXPECT_EQ(0, _stats.filtered_segment_number);
    EXPECT_EQ(0, _stats.expr_zonemap_filtered_segments);
}

TEST_F(SegmentIteratorExprZonemapTest, ApplyExprZonemapPrunesPageRowRanges) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);
    SegmentIterator iter(segment, read_schema);
    iter._file_reader = segment->_file_reader;
    iter._opts.stats = &_stats;
    iter._opts.tablet_schema = _tablet_schema;

    auto expr_ctx = std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(1, 500));
    VExprContextSPtrs conjuncts {expr_ctx};
    auto row_ranges = RowRanges::create_single(kNumRows);

    auto st = iter._apply_expr_zonemap_to_row_ranges(conjuncts, 0, &row_ranges);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_GT(_stats.expr_zonemap_filtered_pages, 0);
    EXPECT_GT(row_ranges.from(), 0);
    EXPECT_LT(row_ranges.count(), kNumRows);
    EXPECT_EQ(kNumRows, row_ranges.to());
}

// A segment with the external column meta region (V3) reads the region once when its column metas
// are created. Whatever first uses the segment lets the region go when it returns, so a cached
// segment never keeps it: an iterator, an empty iterator for a pruned segment, a column reader or a
// meta traversal.
class SegmentMetaRegionReleaseTest : public SegmentIteratorExprZonemapTest {
protected:
    void SetUp() override {
        SegmentIteratorExprZonemapTest::SetUp();
        _tablet_schema = make_tablet_schema(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3);
        _old_max_bytes = config::segment_column_meta_region_buffer_max_bytes;
        config::segment_column_meta_region_buffer_max_bytes = 2 * 1024 * 1024;
        _regions_before = live_meta_regions();
    }
    void TearDown() override {
        config::segment_column_meta_region_buffer_max_bytes = _old_max_bytes;
        SegmentIteratorExprZonemapTest::TearDown();
    }
    static bool region_kept(const std::shared_ptr<Segment>& segment) {
        return segment->_column_meta_accessor->meta_region() != nullptr;
    }

    int64_t _old_max_bytes = 0;
    int64_t _regions_before = 0;
};

TEST_F(SegmentMetaRegionReleaseTest, CreatingColumnMetasReadsTheRegionAndTimesItApart) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    ASSERT_TRUE(segment->_create_column_meta_once(&_stats).ok());
    ASSERT_NE(segment->_column_meta_accessor->meta_region_holder(), nullptr);
    EXPECT_TRUE(region_kept(segment));
    EXPECT_EQ(live_meta_regions() - _regions_before, 1);
    EXPECT_GT(_stats.segment_column_meta_region_read_timer_ns, 0);
    EXPECT_GE(_stats.segment_create_column_readers_timer_ns, 0);
    segment->release_column_meta_region();
    EXPECT_EQ(live_meta_regions(), _regions_before);
}

TEST_F(SegmentMetaRegionReleaseTest, NoRegionReadWithoutABuffer) {
    config::segment_column_meta_region_buffer_max_bytes = 0;
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    ASSERT_TRUE(segment->_create_column_meta_once(&_stats).ok());
    EXPECT_FALSE(region_kept(segment));
    EXPECT_EQ(_stats.segment_column_meta_region_read_timer_ns, 0);
    EXPECT_EQ(live_meta_regions(), _regions_before);
}

TEST_F(SegmentMetaRegionReleaseTest, NewIteratorOfAPrunedSegmentLetsTheRegionGo) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);
    auto expr_ctx = std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(1, 2000));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_ctx));
    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.common_expr_ctxs_push_down = {expr_ctx};

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(iter->empty());
    EXPECT_EQ(1, _stats.expr_zonemap_filtered_segments);
    EXPECT_GT(_stats.segment_column_meta_region_read_timer_ns, 0);
    EXPECT_FALSE(region_kept(segment));
    EXPECT_EQ(live_meta_regions(), _regions_before);
}

TEST_F(SegmentMetaRegionReleaseTest, NewIteratorOfAReadSegmentLetsTheRegionGo) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);
    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_FALSE(iter->empty());
    EXPECT_FALSE(region_kept(segment));
    EXPECT_EQ(live_meta_regions(), _regions_before);
}

TEST_F(SegmentMetaRegionReleaseTest, AColumnReaderAsFirstUseLetsTheRegionGo) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    std::shared_ptr<ColumnReader> reader;
    auto st = segment->get_column_reader(_tablet_schema->column(1), &reader, &_stats);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(reader, nullptr);
    EXPECT_GT(_stats.segment_column_meta_region_read_timer_ns, 0);
    EXPECT_FALSE(region_kept(segment));
    EXPECT_EQ(live_meta_regions(), _regions_before);

    std::shared_ptr<ColumnReader> by_uid;
    st = segment->get_column_reader(_tablet_schema->column(0).unique_id(), &by_uid, &_stats);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(by_uid, nullptr);
}

TEST_F(SegmentMetaRegionReleaseTest, AMetaTraversalAsFirstUseLetsTheRegionGo) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    size_t metas = 0;
    auto st = segment->traverse_column_meta_pbs([&](const ColumnMetaPB&) { ++metas; });
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(metas, _tablet_schema->num_columns());
    EXPECT_FALSE(region_kept(segment));
    EXPECT_EQ(live_meta_regions(), _regions_before);
}

TEST_F(SegmentMetaRegionReleaseTest, AUseAfterTheFirstDoesNotReleaseAnotherCallersRegion) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    // new_iterator creates the metas and then looks column readers up itself; those lookups did
    // not create the metas, so only new_iterator lets the region go, once it is done.
    ASSERT_TRUE(segment->_create_column_meta_once(&_stats).ok());
    std::shared_ptr<ColumnReader> reader;
    auto st = segment->get_column_reader(_tablet_schema->column(1), &reader, &_stats);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_TRUE(region_kept(segment));
    segment->release_column_meta_region();
    EXPECT_FALSE(region_kept(segment));
}

// A segment re-opened after it left the segment cache, with its region already in the local file
// cache, reads only the metas it needs, exactly as without the region buffer; a segment whose region
// is not local yet reads the region in one read.
class SegmentMetaRegionLocalCacheTest : public SegmentMetaRegionReleaseTest {
protected:
    std::shared_ptr<Segment> reopen(bool in_local_cache, std::shared_ptr<LocalCacheProbeReader>* probe) {
        std::shared_ptr<Segment> segment;
        auto st = Segment::open(io::global_local_filesystem(),
                                std::string(kTestDir) + "/expr_zonemap_segment.dat", 100, 0,
                                kRowsetId, _tablet_schema, io::FileReaderOptions {}, &segment);
        EXPECT_TRUE(st.ok()) << st;
        *probe = std::make_shared<LocalCacheProbeReader>(segment->_file_reader, in_local_cache);
        segment->_file_reader = *probe;
        return segment;
    }

    Status read_once(const std::shared_ptr<Segment>& segment, OlapReaderStatistics* stats) {
        StorageReadOptions read_options;
        read_options.stats = stats;
        read_options.runtime_state = &_runtime_state;
        read_options.tablet_schema = _tablet_schema;
        std::unique_ptr<RowwiseIterator> iter;
        return segment->new_iterator(make_read_schema(_tablet_schema), read_options, &iter);
    }
};

TEST_F(SegmentMetaRegionLocalCacheTest, WarmReopenReadsWhatTheUnbufferedPathReads) {
    std::shared_ptr<Segment> written;
    ASSERT_NO_FATAL_FAILURE(build_segment(&written));
    written.reset();

    config::segment_column_meta_region_buffer_max_bytes = 0;
    std::shared_ptr<LocalCacheProbeReader> unbuffered;
    auto segment = reopen(false, &unbuffered);
    OlapReaderStatistics unbuffered_stats;
    ASSERT_TRUE(read_once(segment, &unbuffered_stats).ok());
    segment.reset();

    config::segment_column_meta_region_buffer_max_bytes = 2 * 1024 * 1024;
    const int64_t skips_before = exposed_counter("segment_column_meta_region_local_skips");
    std::shared_ptr<LocalCacheProbeReader> buffered;
    segment = reopen(true, &buffered);
    OlapReaderStatistics buffered_stats;
    ASSERT_TRUE(read_once(segment, &buffered_stats).ok());

    EXPECT_EQ(exposed_counter("segment_column_meta_region_local_skips") - skips_before, 1);
    EXPECT_EQ(buffered->reads, unbuffered->reads);
    EXPECT_EQ(buffered->bytes, unbuffered->bytes);
    EXPECT_EQ(live_meta_regions(), _regions_before);
}

TEST_F(SegmentMetaRegionLocalCacheTest, ColdOpenReadsTheRegionOnce) {
    std::shared_ptr<Segment> written;
    ASSERT_NO_FATAL_FAILURE(build_segment(&written));
    OlapReaderStatistics stats;
    std::shared_ptr<SegmentFooterPB> footer;
    ASSERT_TRUE(written->_get_segment_footer(footer, &stats, nullptr).ok());
    ExternalColMetaUtil::ExternalMetaPointers ptrs;
    ASSERT_TRUE(ExternalColMetaUtil::parse_external_meta_pointers(*footer, &ptrs).ok());
    const size_t region_size = ptrs.region_end - ptrs.region_start;
    ASSERT_GT(region_size, 0);
    written.reset();

    const int64_t skips_before = exposed_counter("segment_column_meta_region_local_skips");
    std::shared_ptr<LocalCacheProbeReader> probe;
    auto segment = reopen(false, &probe);
    ASSERT_TRUE(segment->_create_column_meta_once(&stats).ok());
    EXPECT_TRUE(region_kept(segment));
    EXPECT_GE(probe->largest_read, region_size);
    EXPECT_GT(stats.segment_column_meta_region_read_timer_ns, 0);
    EXPECT_EQ(exposed_counter("segment_column_meta_region_local_skips"), skips_before);
    segment->release_column_meta_region();
}

} // namespace doris::segment_v2
