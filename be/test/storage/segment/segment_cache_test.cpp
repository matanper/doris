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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <bvar/bvar.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdlib.h>
#include <unistd.h>

#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/object_pool.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/data_type/define_primitive_type.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/cast/cast_to_date_or_datetime_impl.hpp"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "load/delta_writer/delta_writer.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "storage/data_dir.h"
#include "storage/iterators.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/schema.h"
#include "storage/segment/column_reader_cache.h"
#include "storage/segment/lazy_init_segment_iterator.h"
#include "storage/segment/segment.h"
#include "storage/cache/page_cache.h"
#include "storage/compaction/compaction.h"
#include "storage/segment/segment_loader.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet_info.h"
#include "storage/task/engine_publish_version_task.h"
#include "storage/txn/txn_manager.h"

namespace doris {
class OlapMeta;

// This is DeltaWriter unit test which used by streaming load.
// And also it should take schema change into account after streaming load.

static const uint32_t MAX_PATH_LEN = 1024;
static StorageEngine* engine_ref = nullptr;

static std::shared_ptr<Schema> create_full_schema(const TabletSchemaSPtr& tablet_schema) {
    size_t num_columns = tablet_schema->num_columns();
    if (num_columns > 0 && tablet_schema->columns().back()->name() == BeConsts::ROW_STORE_COL) {
        --num_columns;
    }

    std::vector<ColumnId> column_ids(num_columns);
    for (uint32_t cid = 0; cid < num_columns; ++cid) {
        column_ids[cid] = cid;
    }
    return std::make_shared<Schema>(tablet_schema->columns(), column_ids);
}

static void set_up() {
    char buffer[MAX_PATH_LEN];
    EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
    config::storage_root_path = std::string(buffer) + "/segment_cache_test";
    auto st = io::global_local_filesystem()->delete_directory(config::storage_root_path);
    ASSERT_TRUE(st.ok()) << st;
    st = io::global_local_filesystem()->create_directory(config::storage_root_path);
    ASSERT_TRUE(st.ok()) << st;
    std::vector<StorePath> paths;
    paths.emplace_back(config::storage_root_path, -1);

    doris::EngineOptions options;
    options.store_paths = paths;
    auto engine = std::make_unique<StorageEngine>(options);
    engine_ref = engine.get();
    Status s = engine->open();
    ASSERT_TRUE(s.ok()) << s;
    ASSERT_TRUE(s.ok()) << s;

    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_memtable_memory_limiter(new MemTableMemoryLimiter());
    exec_env->set_storage_engine(std::move(engine));
}

static void tear_down() {
    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_memtable_memory_limiter(nullptr);
    engine_ref = nullptr;
    exec_env->set_storage_engine(nullptr);
    EXPECT_EQ(system("rm -rf ./segment_cache_test"), 0);
    static_cast<void>(io::global_local_filesystem()->delete_directory(
            std::string(getenv("DORIS_HOME")) + "/" + UNUSED_PREFIX));
}

static void create_tablet_request_with_sequence_col(int64_t tablet_id, int32_t schema_hash,
                                                    TCreateTabletReq* request,
                                                    bool enable_mow = false) {
    request->tablet_id = tablet_id;
    request->__set_version(1);
    request->partition_id = 30004;
    request->tablet_schema.schema_hash = schema_hash;
    request->tablet_schema.short_key_column_count = 2;
    request->tablet_schema.keys_type = TKeysType::UNIQUE_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;
    request->tablet_schema.__set_sequence_col_idx(4);
    request->__set_storage_format(TStorageFormat::V2);
    request->__set_enable_unique_key_merge_on_write(enable_mow);

    TColumn k1;
    k1.column_name = "k1";
    k1.__set_is_key(true);
    k1.column_type.type = TPrimitiveType::TINYINT;
    request->tablet_schema.columns.push_back(k1);

    TColumn k2;
    k2.column_name = "k2";
    k2.__set_is_key(true);
    k2.column_type.type = TPrimitiveType::SMALLINT;
    request->tablet_schema.columns.push_back(k2);

    TColumn v1;
    v1.column_name = "v1";
    v1.__set_is_key(false);
    v1.column_type.type = TPrimitiveType::DATETIME;
    v1.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v1);

    TColumn v2;
    v2.column_name = "v2";
    v2.__set_is_key(false);
    v2.column_type.type = TPrimitiveType::DATEV2;
    v2.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v2);

    TColumn sequence_col;
    sequence_col.column_name = SEQUENCE_COL;
    sequence_col.__set_is_key(false);
    sequence_col.column_type.type = TPrimitiveType::INT;
    sequence_col.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(sequence_col);
}

static TDescriptorTable create_descriptor_tablet_with_sequence_col() {
    TDescriptorTableBuilder dtb;
    TTupleDescriptorBuilder tuple_builder;

    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_TINYINT).column_name("k1").column_pos(0).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_SMALLINT).column_name("k2").column_pos(1).build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_DATETIME)
                                   .column_name("v1")
                                   .column_pos(2)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_DATEV2)
                                   .column_name("v2")
                                   .column_pos(3)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_INT)
                                   .column_name(SEQUENCE_COL)
                                   .column_pos(4)
                                   .nullable(false)
                                   .build());
    tuple_builder.build(&dtb);

    return dtb.desc_tbl();
}

static void generate_data(Block* block, int8_t k1, int16_t k2, int32_t seq) {
    auto columns = std::move(*block).mutate_columns();
    int8_t c1 = k1;
    columns[0]->insert_data((const char*)&c1, sizeof(c1));

    int16_t c2 = k2;
    columns[1]->insert_data((const char*)&c2, sizeof(c2));

    VecDateTimeValue c3;
    {
        CastParameters p;
        CastToDateOrDatetime::from_string_strict_mode<DatelikeParseMode::STRICT,
                                                      DatelikeTargetType::DATE_TIME>(
                {"2020-07-16 19:39:43", 19}, c3, nullptr, p);
    }
    int64_t c3_int = c3.to_int64();
    columns[2]->insert_data((const char*)&c3_int, sizeof(c3_int));

    DateV2Value<DateV2ValueType> c4;
    c4.unchecked_set_time(2022, 6, 6, 0, 0, 0, 0);
    uint32_t c4_int = c4.to_date_int_val();
    columns[3]->insert_data((const char*)&c4_int, sizeof(c4_int));

    int32_t c5 = seq;
    columns[4]->insert_data((const char*)&c5, sizeof(c5));
    block->set_columns(std::move(columns));
}

class SegmentCacheTest : public ::testing::Test {
public:
    SegmentCacheTest() = default;
    ~SegmentCacheTest() = default;
    static void SetUpTestSuite() {
        config::min_file_descriptor_number = 100;
        set_up();
    }

    static void TearDownTestSuite() { tear_down(); }
};

TEST_F(SegmentCacheTest, vec_sequence_col) {
    std::unique_ptr<RuntimeProfile> profile;
    profile = std::make_unique<RuntimeProfile>("CreateTablet");
    TCreateTabletReq request;
    sleep(20);
    create_tablet_request_with_sequence_col(55555, 270068377, &request);
    Status res = engine_ref->create_tablet(request, profile.get());
    ASSERT_TRUE(res.ok());

    TDescriptorTable tdesc_tbl = create_descriptor_tablet_with_sequence_col();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    static_cast<void>(DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl));
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    auto param = std::make_shared<OlapTableSchemaParam>();

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req;
    write_req.tablet_id = 55555;
    write_req.schema_hash = 270068377;
    write_req.txn_id = 20003;
    write_req.partition_id = 30003;
    write_req.load_id = load_id;
    write_req.tuple_desc = tuple_desc;
    write_req.slots = &(tuple_desc->slots());
    write_req.is_high_priority = false;
    write_req.table_schema_param = param;
    profile = std::make_unique<RuntimeProfile>("LoadChannels");
    auto delta_writer =
            std::make_unique<DeltaWriter>(*engine_ref, write_req, profile.get(), TUniqueId {});

    Block block;
    for (const auto& slot_desc : tuple_desc->slots()) {
        block.insert(ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(), slot_desc->type(),
                                           slot_desc->col_name()));
    }

    generate_data(&block, 123, 456, 100);
    res = delta_writer->write(&block, {0});
    EXPECT_TRUE(res.ok());

    generate_data(&block, 123, 456, 90);
    res = delta_writer->write(&block, {1});
    ASSERT_TRUE(res.ok());

    res = delta_writer->close();
    ASSERT_TRUE(res.ok());
    res = delta_writer->wait_flush();
    ASSERT_TRUE(res.ok());
    res = delta_writer->build_rowset();
    ASSERT_TRUE(res.ok());
    res = delta_writer->submit_calc_delete_bitmap_task();
    ASSERT_TRUE(res.ok());
    res = delta_writer->wait_calc_delete_bitmap();
    ASSERT_TRUE(res.ok());
    res = delta_writer->commit_txn(PSlaveTabletNodes());
    ASSERT_TRUE(res.ok());

    // publish version success
    TabletSharedPtr tablet = engine_ref->tablet_manager()->get_tablet(write_req.tablet_id);
    std::cout << "before publish, tablet row nums:" << tablet->num_rows() << std::endl;
    OlapMeta* meta = tablet->data_dir()->get_meta();
    Version version;
    version.first = tablet->get_rowset_with_max_version()->end_version() + 1;
    version.second = tablet->get_rowset_with_max_version()->end_version() + 1;
    std::cout << "start to add rowset version:" << version.first << "-" << version.second
              << std::endl;
    std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
    engine_ref->txn_manager()->get_txn_related_tablets(write_req.txn_id, write_req.partition_id,
                                                       &tablet_related_rs);
    ASSERT_EQ(1, tablet_related_rs.size());

    std::cout << "start to publish txn" << std::endl;
    RowsetSharedPtr rowset = tablet_related_rs.begin()->second;
    TabletPublishStatistics pstats;
    std::shared_ptr<TabletTxnInfo> extend_tablet_txn_info_lifetime = nullptr;
    res = engine_ref->txn_manager()->publish_txn(meta, write_req.partition_id, write_req.txn_id,
                                                 write_req.tablet_id,
                                                 tablet_related_rs.begin()->first.tablet_uid,
                                                 version, &pstats, extend_tablet_txn_info_lifetime);
    ASSERT_TRUE(res.ok());
    std::cout << "start to add inc rowset:" << rowset->rowset_id()
              << ", num rows:" << rowset->num_rows() << ", version:" << rowset->version().first
              << "-" << rowset->version().second << std::endl;
    res = tablet->add_inc_rowset(rowset);
    ASSERT_TRUE(res.ok());
    ASSERT_EQ(1, tablet->num_rows());
    std::vector<segment_v2::SegmentSharedPtr> segments;

    SegmentCacheHandle handle;
    BetaRowsetSharedPtr rowset_ptr = std::dynamic_pointer_cast<BetaRowset>(rowset);

    std::mutex lock;
    {
        // Use lock to make sure only this segment can be loaded by SegmentLoader during this test.
        // SegmeentLoader is singleton. Without this lock, multiple segments will be loaded. Result will be wrong.
        std::lock_guard<std::mutex> l(lock);
        // load segments first
        int64_t start_size = SegmentLoader::instance()->cache_mem_usage();
        res = SegmentLoader::instance()->load_segments(rowset_ptr, &handle, true, true);
        ASSERT_TRUE(res.ok());
        EXPECT_EQ(1, rowset->num_segments());
        EXPECT_EQ(1, handle.get_segments().size());
        EXPECT_TRUE(handle.is_inited());
        segment_v2::SegmentSharedPtr segment_ptr = handle.get_segments()[0];

        // load index and bf second
        res = segment_ptr->load_pk_index_and_bf(nullptr);
        ASSERT_TRUE(res.ok());

        // check cache mem usage equals to segment mem usage
        EXPECT_EQ(SegmentLoader::instance()->cache_mem_usage() - start_size,
                  segment_ptr->meta_mem_usage());
    }

    res = ((BetaRowset*)rowset.get())->load_segments(&segments);
    ASSERT_TRUE(res.ok());
    ASSERT_EQ(1, rowset->num_segments());
    ASSERT_EQ(1, segments.size());

    // read data, verify the data correct
    OlapReaderStatistics stats;
    StorageReadOptions opts;
    opts.stats = &stats;
    opts.tablet_schema = rowset->tablet_schema();

    std::unique_ptr<RowwiseIterator> iter;
    std::shared_ptr<Schema> schema = create_full_schema(rowset->tablet_schema());
    auto s = segments[0]->new_iterator(schema, opts, &iter);
    ASSERT_TRUE(s.ok());
    auto read_block = rowset->tablet_schema()->create_block();
    res = iter->next_batch(&read_block);
    ASSERT_TRUE(res.ok()) << res;
    ASSERT_EQ(1, read_block.rows());
    // get the value from sequence column
    auto seq_v = read_block.get_by_position(4).column->get_int(0);
    ASSERT_EQ(100, seq_v);

    // A compaction that read the rowset drops its segments from the segment cache when it ends,
    // and the cache counts them by the kind of reader that loaded them.
    {
        auto loaded_by_compaction = []() {
            return std::stoll(bvar::Variable::describe_exposed(
                    "segment_cache_loaded_segments_by_compaction"));
        };
        SegmentLoader::instance()->erase_segments(rowset->rowset_id(), rowset->num_segments());
        const int64_t before = loaded_by_compaction();
        io::IOContext io_ctx;
        io_ctx.reader_type = ReaderType::READER_CUMULATIVE_COMPACTION;
        {
            SegmentCacheHandle compaction_handle;
            ASSERT_TRUE(SegmentLoader::instance()
                                ->load_segments(rowset_ptr, &compaction_handle, true, false,
                                                nullptr, &io_ctx)
                                .ok());
            EXPECT_EQ(loaded_by_compaction(), before + 1);
            Compaction::release_input_segments({rowset});
            SegmentCacheHandle probe;
            EXPECT_FALSE(SegmentLoader::instance()->_segment_cache->lookup(
                    SegmentCache::CacheKey(rowset->rowset_id(), 0), &probe));
            // Still held by the reader.
            EXPECT_EQ(loaded_by_compaction(), before + 1);
        }
        segments.clear();
        iter.reset();
        EXPECT_EQ(loaded_by_compaction(), before);
    }

    // The cache charges a segment with the bytes it really holds, re-charges it on a hit once
    // that has grown past the charge, and evicts by that charge.
    {
        auto charged = []() {
            return std::stoll(bvar::Variable::describe_exposed("segment_cache_charged_bytes"));
        };
        // The suite's segment loader is too small to cache anything; give it room.
        auto* lru = SegmentLoader::instance()->_segment_cache->_cache.get();
        const size_t old_capacity = lru->get_capacity();
        lru->set_capacity(64 << 20);
        const double old_ratio = config::segment_cache_recharge_ratio;
        const int64_t old_min_bytes = config::segment_cache_recharge_min_bytes;
        SegmentLoader::instance()->erase_segments(rowset->rowset_id(), rowset->num_segments());
        const int64_t base = charged();

        segment_v2::SegmentSharedPtr seg;
        int64_t first_charge = 0;
        {
            SegmentCacheHandle first;
            ASSERT_TRUE(SegmentLoader::instance()->load_segments(rowset_ptr, &first, true).ok());
            seg = first.get_segments()[0];
            first_charge = seg->cache_charge();
            EXPECT_GE(seg->real_mem_usage(), seg->meta_mem_usage());
            EXPECT_EQ(first_charge, std::max<int64_t>(seg->real_mem_usage(), 1));
            EXPECT_EQ(charged(), base + first_charge);

            // Reading parses the column meta and creates the column readers.
            OlapReaderStatistics seg_stats;
            StorageReadOptions seg_opts;
            seg_opts.stats = &seg_stats;
            seg_opts.tablet_schema = rowset->tablet_schema();
            std::unique_ptr<RowwiseIterator> seg_iter;
            ASSERT_TRUE(seg->new_iterator(schema, seg_opts, &seg_iter).ok());
            auto seg_block = rowset->tablet_schema()->create_block();
            ASSERT_TRUE(seg_iter->next_batch(&seg_block).ok());
            EXPECT_GT(seg->_column_meta_bytes.load(), 0);
            EXPECT_GT(seg->_column_reader_cache->retained_bytes(), 0);
            EXPECT_GT(seg->real_mem_usage(), first_charge);
            EXPECT_EQ(seg->real_mem_usage(), seg->meta_mem_usage() +
                                                     seg->_column_meta_bytes.load() +
                                                     seg->_column_reader_cache->retained_bytes());

            // Growth inside the default slack is not re-charged.
            config::segment_cache_recharge_ratio = 1.0;
            config::segment_cache_recharge_min_bytes = seg->real_mem_usage();
            {
                SegmentCacheHandle hit;
                ASSERT_TRUE(SegmentLoader::instance()->load_segments(rowset_ptr, &hit, true).ok());
                EXPECT_EQ(hit.get_segments()[0].get(), seg.get());
            }
            EXPECT_EQ(seg->cache_charge(), first_charge);
            EXPECT_EQ(charged(), base + first_charge);

            // Past it, the hit re-inserts the segment with its real bytes.
            config::segment_cache_recharge_min_bytes = 0;
            {
                SegmentCacheHandle hit;
                ASSERT_TRUE(SegmentLoader::instance()->load_segments(rowset_ptr, &hit, true).ok());
                EXPECT_EQ(hit.get_segments()[0].get(), seg.get());
            }
            EXPECT_EQ(seg->cache_charge(), seg->real_mem_usage());
            // A handle keeps the segment, not the cache entry, so the replaced entry is gone.
            EXPECT_EQ(charged(), base + seg->cache_charge());
        }
        EXPECT_EQ(charged(), base + seg->cache_charge());
        config::segment_cache_recharge_ratio = old_ratio;
        config::segment_cache_recharge_min_bytes = old_min_bytes;

        // A per-shard cap that fits the open-time charge but not the real one evicts the segment.
        const size_t per_shard = first_charge + 512;
        ASSERT_GT(seg->cache_charge(), static_cast<int64_t>(per_shard));
        lru->set_capacity(per_shard * 64);
        SegmentCacheHandle probe;
        EXPECT_FALSE(SegmentLoader::instance()->_segment_cache->lookup(
                SegmentCache::CacheKey(rowset->rowset_id(), 0), &probe));
        EXPECT_EQ(charged(), base);
        lru->set_capacity(old_capacity);
    }

    // Segment iterators that share a reader's pins open each segment once, even when the cache
    // drops it between them, as a compaction's column groups do.
    {
        auto counter = [](const char* name) {
            return std::stoll(bvar::Variable::describe_exposed(name));
        };
        StorageReadOptions pin_opts;
        OlapReaderStatistics pin_stats;
        pin_opts.stats = &pin_stats;
        pin_opts.tablet_schema = rowset->tablet_schema();
        pin_opts.io_ctx.reader_type = ReaderType::READER_CUMULATIVE_COMPACTION;
        auto read_one = [&](std::shared_ptr<segment_v2::PinnedSegments> pins) {
            SegmentLoader::instance()->erase_segments(rowset->rowset_id(), rowset->num_segments());
            segment_v2::LazyInitSegmentIterator it(rowset_ptr, 0, true, schema, pin_opts,
                                                   std::move(pins));
            ASSERT_TRUE(it.init(pin_opts).ok());
            auto pin_block = rowset->tablet_schema()->create_block();
            ASSERT_TRUE(it.next_batch(&pin_block).ok());
            EXPECT_EQ(1, pin_block.rows());
        };
        const int64_t loads = counter("segment_loads_by_compaction");
        const int64_t lookups = counter("segment_cache_lookups_by_compaction");
        const int64_t reuses = counter("segment_pinned_reuses");

        auto pins = std::make_shared<segment_v2::PinnedSegments>();
        read_one(pins);
        read_one(pins);
        read_one(pins);
        EXPECT_EQ(counter("segment_loads_by_compaction"), loads + 1);
        EXPECT_EQ(counter("segment_cache_lookups_by_compaction"), lookups + 1);
        EXPECT_EQ(counter("segment_pinned_reuses"), reuses + 2);
        ASSERT_EQ(pins->segments.size(), 1);

        // Without pins every iterator opens the segment again.
        read_one(nullptr);
        read_one(nullptr);
        EXPECT_EQ(counter("segment_loads_by_compaction"), loads + 3);
        EXPECT_EQ(counter("segment_pinned_reuses"), reuses + 2);
        SegmentLoader::instance()->erase_segments(rowset->rowset_id(), rowset->num_segments());
    }

    // Erasing a rowset's segments can drop their cached footers too.
    {
        auto* lru = SegmentLoader::instance()->_segment_cache->_cache.get();
        const size_t old_capacity = lru->get_capacity();
        lru->set_capacity(64 << 20);
        auto* footer_cache = ExecEnv::GetInstance()->get_storage_page_cache();
        SegmentLoader::instance()->erase_segments(rowset->rowset_id(), rowset->num_segments());
        SegmentCacheHandle cached;
        ASSERT_TRUE(SegmentLoader::instance()->load_segments(rowset_ptr, &cached, true).ok());
        auto seg = cached.get_segments()[0];
        std::shared_ptr<segment_v2::SegmentFooterPB> footer;
        ASSERT_TRUE(seg->_get_segment_footer(footer, nullptr).ok());
        const auto footer_key = seg->get_segment_footer_cache_key();
        PageCacheHandle page;
        EXPECT_TRUE(footer_cache->lookup(footer_key, &page, segment_v2::PageTypePB::INDEX_PAGE));

        SegmentLoader::instance()->erase_segments(rowset->rowset_id(), rowset->num_segments());
        EXPECT_TRUE(footer_cache->lookup(footer_key, &page, segment_v2::PageTypePB::INDEX_PAGE))
                << "kept unless asked";
        ASSERT_TRUE(SegmentLoader::instance()->load_segments(rowset_ptr, &cached, true).ok());
        SegmentCacheHandle again;
        ASSERT_TRUE(SegmentLoader::instance()->load_segments(rowset_ptr, &again, true).ok());
        SegmentLoader::instance()->erase_segments(rowset->rowset_id(), rowset->num_segments(),
                                                  true);
        EXPECT_FALSE(footer_cache->lookup(footer_key, &page, segment_v2::PageTypePB::INDEX_PAGE));
        lru->set_capacity(old_capacity);
    }

    res = engine_ref->tablet_manager()->drop_tablet(request.tablet_id, request.replica_id, false);
    ASSERT_TRUE(res.ok());
}

} // namespace doris
