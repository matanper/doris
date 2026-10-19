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
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "cloud/cloud_cluster_info.h"
#include "cloud/cloud_cumulative_compaction.h"
#include "cloud/cloud_full_compaction.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/config.h"
#include "common/config.h"
#include "cpp/sync_point.h"
#include "runtime/exec_env.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet/tablet_meta.h"
#include "util/defer_op.h"
#include "util/time.h"

namespace doris {

class CloudFinalCompactionTest : public testing::Test {
protected:
    CloudFinalCompactionTest() : _engine(EngineOptions {}) {}

    void SetUp() override {
        _cluster_info = std::make_shared<CloudClusterInfo>();
        _cluster_info->_is_in_standby = false;
        ExecEnv::GetInstance()->_cluster_info = _cluster_info.get();
        auto* sp = SyncPoint::get_instance();
        sp->clear_all_call_backs();
        sp->enable_processing();
    }

    void TearDown() override {
        auto* sp = SyncPoint::get_instance();
        sp->clear_all_call_backs();
        sp->disable_processing();
    }

    CloudTabletSPtr make_tablet(int64_t tablet_id) {
        auto tablet_meta = std::make_shared<TabletMeta>(
                1, 2, tablet_id, 15674, 4, 5, TTabletSchema(), 6,
                std::unordered_map<uint32_t, uint32_t> {{7, 8}}, UniqueId(9, 10),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F);
        tablet_meta->set_tablet_state(TABLET_RUNNING);
        auto tablet = std::make_shared<CloudTablet>(_engine, tablet_meta);
        tablet->tablet_meta()->tablet_schema()->set_disable_auto_compaction(false);
        return tablet;
    }

    static RowsetSharedPtr make_rowset(int64_t start, int64_t end) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->set_rowset_type(BETA_ROWSET);
        rs_meta->_rowset_meta_pb.set_start_version(start);
        rs_meta->_rowset_meta_pb.set_end_version(end);
        rs_meta->set_num_segments(1);
        rs_meta->set_segments_overlap(NONOVERLAPPING);
        rs_meta->set_total_disk_size(1024);
        RowsetSharedPtr rowset;
        EXPECT_TRUE(RowsetFactory::create_rowset(nullptr, "", rs_meta, &rowset).ok());
        return rowset;
    }

    // Makes every following prepare_tablet_job call answer with `code`.
    static void answer_prepare_tablet_job(cloud::MetaServiceCode code) {
        SyncPoint::get_instance()->set_call_back(
                "CloudMetaMgr::prepare_tablet_job", [code](auto&& outcome) {
                    auto* pairs = try_any_cast_ret<Status>(outcome);
                    pairs->second = true;
                    if (code == cloud::OK) {
                        pairs->first = Status::OK();
                        return;
                    }
                    pairs->first = Status::InternalError<false>("injected meta service error");
                    auto* resp = try_any_cast<cloud::StartTabletJobResponse*>(outcome[1]);
                    resp->mutable_status()->set_code(code);
                });
    }

    CloudStorageEngine _engine;
    std::shared_ptr<CloudClusterInfo> _cluster_info;
};

TEST_F(CloudFinalCompactionTest, busy_backoff_doubles_up_to_cap) {
    EXPECT_EQ(CloudTablet::cumu_compaction_busy_backoff_ms(1, 5000, 300000), 5000);
    EXPECT_EQ(CloudTablet::cumu_compaction_busy_backoff_ms(2, 5000, 300000), 10000);
    EXPECT_EQ(CloudTablet::cumu_compaction_busy_backoff_ms(3, 5000, 300000), 20000);
    EXPECT_EQ(CloudTablet::cumu_compaction_busy_backoff_ms(7, 5000, 300000), 300000);
    EXPECT_EQ(CloudTablet::cumu_compaction_busy_backoff_ms(1000, 5000, 300000), 300000);
    // A cap at or below the base interval keeps the fixed interval.
    EXPECT_EQ(CloudTablet::cumu_compaction_busy_backoff_ms(10, 5000, 5000), 5000);
    EXPECT_EQ(CloudTablet::cumu_compaction_busy_backoff_ms(10, 5000, 1000), 5000);
    EXPECT_EQ(CloudTablet::cumu_compaction_busy_backoff_ms(10, 0, 1000), 0);
    EXPECT_EQ(CloudTablet::cumu_compaction_busy_backoff_ms(70, 1, INT64_MAX), INT64_MAX);
}

TEST_F(CloudFinalCompactionTest, busy_job_backs_off_tablet_until_a_job_starts) {
    auto old_cap = config::cumu_compaction_busy_max_backoff_ms;
    auto old_min = config::min_compaction_failure_interval_ms;
    Defer restore {[&]() {
        config::cumu_compaction_busy_max_backoff_ms = old_cap;
        config::min_compaction_failure_interval_ms = old_min;
    }};
    config::cumu_compaction_busy_max_backoff_ms = 300000;
    config::min_compaction_failure_interval_ms = 5000;

    auto tablet = make_tablet(21001);
    CloudCumulativeCompaction compaction(_engine, tablet);
    compaction._input_rowsets = {make_rowset(2, 2), make_rowset(3, 3)};
    int64_t now = UnixMillis();

    answer_prepare_tablet_job(cloud::JOB_TABLET_BUSY);
    EXPECT_FALSE(compaction.request_global_lock().ok());
    EXPECT_TRUE(tablet->in_cumu_compaction_busy_backoff(now + 4000));
    EXPECT_FALSE(compaction.request_global_lock().ok());
    EXPECT_TRUE(tablet->in_cumu_compaction_busy_backoff(now + 9000));
    EXPECT_FALSE(compaction.request_global_lock().ok());
    EXPECT_TRUE(tablet->in_cumu_compaction_busy_backoff(now + 19000));

    answer_prepare_tablet_job(cloud::OK);
    EXPECT_TRUE(compaction.request_global_lock().ok());
    EXPECT_FALSE(tablet->in_cumu_compaction_busy_backoff(now));

    // The streak starts over after the job that got through.
    answer_prepare_tablet_job(cloud::JOB_TABLET_BUSY);
    EXPECT_FALSE(compaction.request_global_lock().ok());
    EXPECT_FALSE(tablet->in_cumu_compaction_busy_backoff(UnixMillis() + 6000));
}

TEST_F(CloudFinalCompactionTest, topn_skips_tablet_in_busy_backoff) {
    CloudTabletMgr mgr(_engine);
    auto tablet = make_tablet(21002);
    tablet->_approximate_cumu_num_deltas = 5000;
    mgr.put_tablet_for_UT(tablet);
    auto filter_out = [](CloudTablet*) { return false; };
    std::vector<std::shared_ptr<CloudTablet>> picked;
    int64_t max_score = 0;

    tablet->on_cumu_compaction_busy(UnixMillis());
    ASSERT_TRUE(mgr.get_topn_tablets_to_compact(1, CompactionType::CUMULATIVE_COMPACTION,
                                                filter_out, &picked, &max_score)
                        .ok());
    EXPECT_TRUE(picked.empty());

    tablet->reset_cumu_compaction_busy();
    ASSERT_TRUE(mgr.get_topn_tablets_to_compact(1, CompactionType::CUMULATIVE_COMPACTION,
                                                filter_out, &picked, &max_score)
                        .ok());
    EXPECT_EQ(picked.size(), 1);
}

TEST_F(CloudFinalCompactionTest, cumu_skips_tablet_with_submitted_full_compaction) {
    auto old_parallel = config::enable_parallel_cumu_compaction;
    Defer restore {[&]() { config::enable_parallel_cumu_compaction = old_parallel; }};
    auto tablet = make_tablet(21003);
    tablet->_approximate_cumu_num_deltas = 5000;
    _engine.tablet_mgr().put_tablet_for_UT(tablet);

    for (bool parallel : {false, true}) {
        config::enable_parallel_cumu_compaction = parallel;
        _engine._submitted_full_compactions.emplace(tablet->tablet_id(), nullptr);
        auto picked = _engine._generate_cloud_compaction_tasks(
                CompactionType::CUMULATIVE_COMPACTION, true);
        EXPECT_TRUE(picked.empty()) << "parallel=" << parallel;

        _engine._submitted_full_compactions.clear();
        picked = _engine._generate_cloud_compaction_tasks(CompactionType::CUMULATIVE_COMPACTION,
                                                          true);
        EXPECT_EQ(picked.size(), 1) << "parallel=" << parallel;
    }
}

TEST_F(CloudFinalCompactionTest, cancel_marks_submitted_full_compaction) {
    auto tablet = make_tablet(21004);
    EXPECT_TRUE(_engine.cancel_full_compaction(21004).is<ErrorCode::NOT_FOUND>());

    // Still being prepared: nothing to mark yet.
    _engine._submitted_full_compactions.emplace(21004, nullptr);
    EXPECT_FALSE(_engine.cancel_full_compaction(21004).ok());
    EXPECT_FALSE(tablet->compaction_cancel_requested());

    _engine._submitted_full_compactions[21004] =
            std::make_shared<CloudFullCompaction>(_engine, tablet);
    EXPECT_TRUE(_engine.cancel_full_compaction(21004).ok());
    EXPECT_TRUE(tablet->compaction_cancel_requested());
    _engine._submitted_full_compactions.clear();
}

TEST_F(CloudFinalCompactionTest, full_compaction_output_cache_follows_config) {
    auto old_keep = config::enable_file_cache_keep_full_compaction_output;
    auto old_index_only = config::enable_file_cache_write_index_file_only;
    Defer restore {[&]() {
        config::enable_file_cache_keep_full_compaction_output = old_keep;
        config::enable_file_cache_write_index_file_only = old_index_only;
    }};
    config::enable_file_cache_write_index_file_only = false;
    auto tablet = make_tablet(21005);
    CloudFullCompaction compaction(_engine, tablet);

    config::enable_file_cache_keep_full_compaction_output = false;
    EXPECT_FALSE(compaction.should_cache_compaction_output());
    config::enable_file_cache_keep_full_compaction_output = true;
    EXPECT_TRUE(compaction.should_cache_compaction_output());
}

} // namespace doris
