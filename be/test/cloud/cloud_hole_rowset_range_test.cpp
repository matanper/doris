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
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <random>
#include <set>
#include <thread>
#include <vector>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet/tablet_meta.h"

namespace doris {

using VersionSet = std::set<std::pair<int64_t, int64_t>>;

// Version holes of a cloud tablet (versions whose loads wrote other tablets) are filled with
// empty hole rowsets. These tests cover filling a run of holes with one rowset covering the run.
class CloudHoleRowsetRangeTest : public testing::Test {
public:
    CloudHoleRowsetRangeTest() : _engine(CloudStorageEngine(EngineOptions {})) {}

    void SetUp() override {
        _saved_enable = config::enable_hole_rowset_version_range;
        _saved_max_versions = config::hole_rowset_max_versions;
        _saved_eager_prefixes = config::hole_rowset_eager_prefix_versions;
        config::enable_hole_rowset_version_range = true;
        config::hole_rowset_max_versions = 128;
        config::hole_rowset_eager_prefix_versions = 16;
        _tablet = make_tablet(false);
    }

    void TearDown() override {
        config::enable_hole_rowset_version_range = _saved_enable;
        config::hole_rowset_max_versions = _saved_max_versions;
        config::hole_rowset_eager_prefix_versions = _saved_eager_prefixes;
    }

    std::shared_ptr<CloudTablet> make_tablet(bool merge_on_write) {
        auto tablet_meta = std::make_shared<TabletMeta>(
                1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6,
                std::unordered_map<uint32_t, uint32_t> {{7, 8}}, UniqueId(9, 10),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0, merge_on_write);
        tablet_meta->set_tablet_state(TABLET_RUNNING);
        return std::make_shared<CloudTablet>(_engine, tablet_meta);
    }

    RowsetSharedPtr create_rowset(Version version, int num_segments = 1) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->set_rowset_type(BETA_ROWSET);
        rs_meta->set_version(version);
        rs_meta->set_rowset_id(_engine.next_rowset_id());
        rs_meta->set_num_segments(num_segments);
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        auto* col = schema_pb.add_column();
        col->set_unique_id(0);
        col->set_name("k1");
        col->set_type("INT");
        col->set_is_key(true);
        col->set_is_nullable(false);
        rs_meta->set_tablet_schema(schema_pb);
        RowsetSharedPtr rowset;
        EXPECT_TRUE(RowsetFactory::create_rowset(nullptr, "", rs_meta, &rowset).ok());
        return rowset;
    }

    void add_rowsets(CloudTablet* tablet, std::vector<RowsetSharedPtr> rowsets, bool overlap) {
        std::unique_lock wlock(tablet->get_header_lock());
        tablet->add_rowsets(std::move(rowsets), overlap, wlock, false);
    }

    void add_empty_pending(CloudTablet* tablet, int64_t version) {
        std::lock_guard<std::mutex> lock(tablet->_visible_pending_rs_lock);
        tablet->_visible_pending_rs_map.emplace(
                version, CloudTablet::VisiblePendingRowset {nullptr, INT64_MAX, true});
    }

    void add_real_pending(CloudTablet* tablet, int64_t version) {
        auto rs = create_rowset(Version(version, version));
        std::lock_guard<std::mutex> lock(tablet->_visible_pending_rs_lock);
        tablet->_visible_pending_rs_map.emplace(
                version, CloudTablet::VisiblePendingRowset {rs->rowset_meta(), INT64_MAX, false});
    }

    Status fill_version_holes(CloudTablet* tablet, int64_t max_version) {
        std::unique_lock wlock(tablet->get_header_lock());
        return _engine.meta_mgr().fill_version_holes(tablet, max_version, wlock);
    }

    static VersionSet live_versions(const CloudTablet& tablet) {
        VersionSet versions;
        for (const auto& [v, _] : tablet.rowset_map()) {
            versions.emplace(v.first, v.second);
        }
        return versions;
    }

    // Every version in [1, max_version] must be readable through the version graph, and the
    // rowsets on the captured path must cover [0, v] exactly.
    static void expect_all_versions_capturable(const CloudTablet& tablet) {
        // The visible version index and the delta counts follow the live rowsets only; stale
        // hole rowsets stay out of them.
        const auto& rs_metas = const_cast<CloudTablet&>(tablet).tablet_meta()->all_rs_metas();
        ASSERT_EQ(tablet._visible_version_index.size(), rs_metas.size());
        for (const auto& [v, _] : rs_metas) {
            ASSERT_TRUE(tablet._visible_version_index.contains(v)) << v.to_string();
        }
        ASSERT_EQ(tablet._visible_rowset_deltas.size(), tablet.rowset_map().size());
        for (const auto& [v, _] : tablet._stale_rs_version_map) {
            ASSERT_FALSE(tablet._visible_version_index.contains(v) && !rs_metas.contains(v))
                    << v.to_string();
        }
        // A version strictly inside a data rowset (a compaction output) is not readable on its
        // own; only hole rowsets can end a read path in the middle of their range.
        std::set<int64_t> inside_data_rowsets;
        for (const auto& [version, rs] : tablet.rowset_map()) {
            if (!rs->is_hole_rowset()) {
                for (int64_t v = version.first; v < version.second; ++v) {
                    inside_data_rowsets.insert(v);
                }
            }
        }
        for (int64_t v = 1; v <= tablet.max_version_unlocked(); ++v) {
            if (inside_data_rowsets.contains(v)) {
                continue;
            }
            auto path = tablet.capture_consistent_versions_unlocked(Version(0, v), {});
            ASSERT_TRUE(path.has_value()) << "version " << v << ": " << path.error();
            auto captured = tablet.capture_consistent_rowsets_unlocked(Version(0, v), {});
            ASSERT_TRUE(captured.has_value()) << "version " << v << ": " << captured.error();
            ASSERT_EQ(captured.value().rowsets.size(), path.value().size()) << "version " << v;
            int64_t next = 0;
            for (size_t i = 0; i < path.value().size(); ++i) {
                const auto& version = path.value()[i];
                ASSERT_EQ(version.first, next) << "version " << v;
                ASSERT_EQ(captured.value().rowsets[i]->version(), version) << "version " << v;
                next = version.second + 1;
            }
            ASSERT_EQ(next, v + 1);
        }
    }

protected:
    CloudStorageEngine _engine;
    std::shared_ptr<CloudTablet> _tablet;
    bool _saved_enable = true;
    int64_t _saved_max_versions = 128;
    int64_t _saved_eager_prefixes = 16;
};

TEST_F(CloudHoleRowsetRangeTest, ApplyPendingCoalescesEmptyRun) {
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    for (int64_t v = 2; v <= 5; ++v) {
        add_empty_pending(_tablet.get(), v);
    }
    _tablet->apply_visible_pending_rowsets();

    EXPECT_EQ(_tablet->max_version_unlocked(), 5);
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 5}}));
    EXPECT_TRUE(_tablet->get_rowset_by_version({2, 5})->is_hole_rowset());
    expect_all_versions_capturable(*_tablet);
}

TEST_F(CloudHoleRowsetRangeTest, ApplyPendingExtendsTrailingHole) {
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    // One publish per version, the production pattern of a tablet that most loads do not write.
    for (int64_t v = 2; v <= 40; ++v) {
        add_empty_pending(_tablet.get(), v);
        _tablet->apply_visible_pending_rowsets();
        ASSERT_EQ(_tablet->max_version_unlocked(), v);
        ASSERT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, v}}));
    }
    expect_all_versions_capturable(*_tablet);
}

// A tablet that most loads do not write keeps one hole rowset as its versions advance: each
// longer range replaces the trailing one, which is dropped instead of kept on the stale path, and
// every version stays readable.
TEST_F(CloudHoleRowsetRangeTest, ExtendedTailIsDroppedNotStaled) {
    config::hole_rowset_eager_prefix_versions = 0;
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    const auto dropped_before =
            std::stoll(bvar::Variable::describe_exposed("hole_rowset_tail_dropped_count"));
    for (int64_t v = 2; v <= 40; ++v) {
        add_empty_pending(_tablet.get(), v);
        _tablet->apply_visible_pending_rowsets();
        ASSERT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, v}}));
        EXPECT_TRUE(_tablet->_stale_rs_version_map.empty()) << v;
        EXPECT_EQ(_tablet->tablet_meta()->all_rs_metas().size(), 2) << v;
    }
    EXPECT_EQ(std::stoll(bvar::Variable::describe_exposed("hole_rowset_tail_dropped_count")) -
                      dropped_before,
              38);
    EXPECT_TRUE(_tablet->_timestamped_version_tracker._stale_version_path_map.empty());
    expect_all_versions_capturable(*_tablet);
}

// With the drop disabled the replaced trailing hole rowsets stay on the stale path, as before.
TEST_F(CloudHoleRowsetRangeTest, ExtendedTailIsStaledWhenDropDisabled) {
    config::hole_rowset_eager_prefix_versions = 0;
    config::hole_rowset_drop_extended_tail = false;
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    for (int64_t v = 2; v <= 10; ++v) {
        add_empty_pending(_tablet.get(), v);
        _tablet->apply_visible_pending_rowsets();
    }
    EXPECT_EQ(_tablet->_stale_rs_version_map.size(), 8);
    expect_all_versions_capturable(*_tablet);
    config::hole_rowset_drop_extended_tail = true;
}

// A trailing hole rowset a reader holds is not extended, so it is neither dropped nor replaced.
TEST_F(CloudHoleRowsetRangeTest, HeldTailIsNotDropped) {
    config::hole_rowset_eager_prefix_versions = 0;
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    add_empty_pending(_tablet.get(), 2);
    _tablet->apply_visible_pending_rowsets();
    auto held = _tablet->get_rowset_by_version({2, 2});
    ASSERT_NE(held, nullptr);
    add_empty_pending(_tablet.get(), 3);
    _tablet->apply_visible_pending_rowsets();
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 2}, {3, 3}}));
    expect_all_versions_capturable(*_tablet);
}

TEST_F(CloudHoleRowsetRangeTest, ApplyPendingMixesRealAndEmptyVersions) {
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    add_empty_pending(_tablet.get(), 2);
    add_empty_pending(_tablet.get(), 3);
    add_real_pending(_tablet.get(), 4);
    add_empty_pending(_tablet.get(), 5);
    add_empty_pending(_tablet.get(), 6);
    _tablet->apply_visible_pending_rowsets();

    EXPECT_EQ(_tablet->max_version_unlocked(), 6);
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 3}, {4, 4}, {5, 6}}));
    EXPECT_FALSE(_tablet->get_rowset_by_version({4, 4})->is_hole_rowset());
    expect_all_versions_capturable(*_tablet);

    // A real version ends the trailing hole; the next empty version starts a new one.
    add_real_pending(_tablet.get(), 7);
    add_empty_pending(_tablet.get(), 8);
    _tablet->apply_visible_pending_rowsets();
    add_empty_pending(_tablet.get(), 9);
    _tablet->apply_visible_pending_rowsets();
    EXPECT_EQ(live_versions(*_tablet),
              (VersionSet {{0, 1}, {2, 3}, {4, 4}, {5, 6}, {7, 7}, {8, 9}}));
    expect_all_versions_capturable(*_tablet);
}

TEST_F(CloudHoleRowsetRangeTest, HoleHeldByCompactionIsNotExtended) {
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    add_empty_pending(_tablet.get(), 2);
    _tablet->apply_visible_pending_rowsets();
    // A compaction holding the trailing hole rowset as an input pins it.
    auto held = _tablet->get_rowset_by_version({2, 2});
    ASSERT_NE(held, nullptr);
    add_empty_pending(_tablet.get(), 3);
    _tablet->apply_visible_pending_rowsets();
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 2}, {3, 3}}));
    held.reset();
    add_empty_pending(_tablet.get(), 4);
    _tablet->apply_visible_pending_rowsets();
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 2}, {3, 4}}));
    expect_all_versions_capturable(*_tablet);
}

TEST_F(CloudHoleRowsetRangeTest, HoleRangeIsCappedByMaxVersions) {
    config::hole_rowset_max_versions = 4;
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 11).ok());
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 5}, {6, 9}, {10, 11}}));
    for (int64_t v = 12; v <= 14; ++v) {
        add_empty_pending(_tablet.get(), v);
        _tablet->apply_visible_pending_rowsets();
    }
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 5}, {6, 9}, {10, 13}, {14, 14}}));
    expect_all_versions_capturable(*_tablet);
}

TEST_F(CloudHoleRowsetRangeTest, FillVersionHolesCoalescesEachRun) {
    add_rowsets(_tablet.get(),
                {create_rowset(Version(0, 1)), create_rowset(Version(5, 5)),
                 create_rowset(Version(9, 12))},
                false);
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 20).ok());
    EXPECT_EQ(live_versions(*_tablet),
              (VersionSet {{0, 1}, {2, 4}, {5, 5}, {6, 8}, {9, 12}, {13, 20}}));
    expect_all_versions_capturable(*_tablet);

    // A later sync with a higher partition version extends the trailing hole rowset.
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 25).ok());
    EXPECT_EQ(live_versions(*_tablet),
              (VersionSet {{0, 1}, {2, 4}, {5, 5}, {6, 8}, {9, 12}, {13, 25}}));
    expect_all_versions_capturable(*_tablet);
}

TEST_F(CloudHoleRowsetRangeTest, DisabledKeepsOneHolePerVersion) {
    config::enable_hole_rowset_version_range = false;
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 4).ok());
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 2}, {3, 3}, {4, 4}}));
    add_empty_pending(_tablet.get(), 5);
    _tablet->apply_visible_pending_rowsets();
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}}));
}

TEST_F(CloudHoleRowsetRangeTest, MergeOnWriteKeepsOneHolePerVersion) {
    auto mow_tablet = make_tablet(true);
    add_rowsets(mow_tablet.get(), {create_rowset(Version(0, 1))}, false);
    ASSERT_TRUE(fill_version_holes(mow_tablet.get(), 4).ok());
    EXPECT_EQ(live_versions(*mow_tablet), (VersionSet {{0, 1}, {2, 2}, {3, 3}, {4, 4}}));
}

TEST_F(CloudHoleRowsetRangeTest, NotRunningTabletKeepsOneHolePerVersion) {
    _tablet->tablet_meta()->set_tablet_state(TABLET_NOTREADY);
    _tablet->set_alter_version(2);
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 5).ok());
    // Versions <= alter_version are skipped for a schema change tablet.
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {3, 3}, {4, 4}, {5, 5}}));
}

// A rowset from meta-service follows the version boundaries of the BE that produced it, so it can
// cover only part of a local hole range.
TEST_F(CloudHoleRowsetRangeTest, MetaServiceRowsetSplitsHoleRange) {
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 10).ok());
    ASSERT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 10}}));

    add_rowsets(_tablet.get(), {create_rowset(Version(5, 7), 0)}, true);
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 4}, {5, 7}, {8, 10}}));
    EXPECT_FALSE(_tablet->get_rowset_by_version({5, 7})->is_hole_rowset());
    expect_all_versions_capturable(*_tablet);

    // Starting at the hole's first version and ending past it.
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 15).ok());
    ASSERT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 4}, {5, 7}, {8, 15}}));
    add_rowsets(_tablet.get(), {create_rowset(Version(2, 9), 0)}, true);
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 9}, {10, 15}}));
    expect_all_versions_capturable(*_tablet);

    // The version graph rebuilt from tablet meta still reaches every version.
    _tablet->_timestamped_version_tracker.construct_versioned_tracker(
            _tablet->tablet_meta()->all_rs_metas(), _tablet->tablet_meta()->all_stale_rs_metas());
    expect_all_versions_capturable(*_tablet);
}

// A live rowset whose version is also held by a stale hole rowset is dropped rather than put on
// the stale path a second time.
TEST_F(CloudHoleRowsetRangeTest, DeleteRowsetWithStaleHoleVersionIsNotStaledTwice) {
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 6).ok()); // live [2-6], stale [2-2]..[2-5]
    ASSERT_NE(_tablet->get_stale_rowset_by_version({2, 4}), nullptr);

    // White-box: a live rowset with the version of a stale hole rowset.
    auto live = create_rowset(Version(2, 4), 0);
    {
        std::unique_lock wlock(_tablet->get_header_lock());
        std::vector<RowsetSharedPtr> to_add {live};
        _tablet->_add_rowsets_directly(to_add, false);
    }
    auto stale_before = _tablet->_stale_rs_version_map.size();
    auto paths_before = _tablet->_timestamped_version_tracker._stale_version_path_map.size();
    {
        std::unique_lock wlock(_tablet->get_header_lock());
        _tablet->delete_rowsets({live}, wlock);
    }
    EXPECT_EQ(_tablet->_stale_rs_version_map.size(), stale_before);
    EXPECT_EQ(_tablet->_timestamped_version_tracker._stale_version_path_map.size(), paths_before);
    EXPECT_TRUE(_tablet->get_stale_rowset_by_version({2, 4})->is_hole_rowset());
    EXPECT_FALSE(_tablet->rowset_map().contains(Version(2, 4)));
    EXPECT_TRUE(_tablet->_unused_rowsets.contains(live->rowset_id()));
    expect_all_versions_capturable(*_tablet);
}

TEST_F(CloudHoleRowsetRangeTest, SplitRemovesCrossingStaleHoleRowsets) {
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 10).ok()); // live [2-10], stale [2-2]..[2-9]
    add_rowsets(_tablet.get(), {create_rowset(Version(5, 7), 0)}, true);

    // No stale hole rowset may cross a live rowset boundary, or the greedy capture can stop
    // inside a live rowset.
    for (const auto& [stale_version, _] : _tablet->_stale_rs_version_map) {
        bool nested = false;
        for (const auto& [live_version, __] : _tablet->rowset_map()) {
            nested |= live_version.contains(stale_version);
        }
        EXPECT_TRUE(nested) << stale_version.to_string();
    }
    // The stale path holds exactly the stale rowsets.
    size_t path_versions = 0;
    for (const auto& [_, path] : _tablet->_timestamped_version_tracker._stale_version_path_map) {
        for (const auto& v : path->timestamped_versions()) {
            EXPECT_TRUE(_tablet->_stale_rs_version_map.contains(v->version()))
                    << v->version().to_string();
            ++path_versions;
        }
    }
    EXPECT_EQ(path_versions, _tablet->_stale_rs_version_map.size());
    EXPECT_EQ(_tablet->tablet_meta()->all_stale_rs_metas().size(),
              _tablet->_stale_rs_version_map.size());
}

} // namespace doris

namespace doris {

class CloudHolePrefixTest : public CloudHoleRowsetRangeTest {
public:
    // [0, 1] and a hole rowset range [2, 101].
    void make_range() {
        add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
        ASSERT_TRUE(fill_version_holes(_tablet.get(), 101).ok());
        ASSERT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 101}}));
    }

    static size_t stale_count(const CloudTablet& tablet) {
        return tablet._stale_rs_version_map.size();
    }

    std::vector<RowsetSharedPtr> read(int64_t version) {
        std::shared_lock rlock(_tablet->get_header_lock());
        auto captured = _tablet->capture_consistent_rowsets_unlocked(Version(0, version), {});
        EXPECT_TRUE(captured.has_value()) << "version " << version;
        return captured.has_value() ? captured.value().rowsets : std::vector<RowsetSharedPtr> {};
    }
};

TEST_F(CloudHolePrefixTest, OnlyTheLastVersionsGetEagerPrefixes) {
    make_range();
    // Prefixes [2, v] for v in [85, 100] only.
    EXPECT_EQ(stale_count(*_tablet), 16);
    for (int64_t v = 85; v <= 100; ++v) {
        EXPECT_TRUE(_tablet->_stale_rs_version_map.contains(Version(2, v))) << v;
    }
    expect_all_versions_capturable(*_tablet);
}

TEST_F(CloudHolePrefixTest, RangesBeforeTheTabletTailGetNoEagerPrefixes) {
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1)), create_rowset(Version(50, 50))},
                false);
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 100).ok());
    ASSERT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 49}, {50, 50}, {51, 100}}));
    // Only versions [84, 99] of the trailing range; none for [2, 49].
    EXPECT_EQ(stale_count(*_tablet), 16);
    for (int64_t v = 84; v <= 99; ++v) {
        EXPECT_TRUE(_tablet->_stale_rs_version_map.contains(Version(51, v))) << v;
    }
    expect_all_versions_capturable(*_tablet);

    // Many short ranges, as a backfilled tablet has: none of them before the tail.
    auto tablet = make_tablet(false);
    std::vector<RowsetSharedPtr> data {create_rowset(Version(0, 1))};
    for (int64_t v = 10; v <= 200; v += 10) {
        data.push_back(create_rowset(Version(v, v)));
    }
    add_rowsets(tablet.get(), data, false);
    ASSERT_TRUE(fill_version_holes(tablet.get(), 200).ok());
    // Prefixes [181, 184..188] and [191, 191..198].
    EXPECT_EQ(stale_count(*tablet), 13);
    expect_all_versions_capturable(*tablet);
}

TEST_F(CloudHolePrefixTest, NoEagerPrefixesStillReadsEveryVersion) {
    config::hole_rowset_eager_prefix_versions = 0;
    make_range();
    EXPECT_EQ(stale_count(*_tablet), 0);
    expect_all_versions_capturable(*_tablet);
    // Reading made no rowsets the tablet keeps.
    EXPECT_EQ(stale_count(*_tablet), 0);
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 101}}));
}

TEST_F(CloudHolePrefixTest, ReadInsideRangeGetsAnEmptyPrefix) {
    make_range();
    auto rowsets = read(50);
    ASSERT_EQ(rowsets.size(), 2);
    EXPECT_EQ(rowsets[0]->version(), Version(0, 1));
    EXPECT_EQ(rowsets[1]->version(), Version(2, 50));
    EXPECT_TRUE(rowsets[1]->is_hole_rowset());
    EXPECT_EQ(rowsets[1]->num_rows(), 0);
    EXPECT_EQ(rowsets[1]->num_segments(), 0);
    // The same read as an eager prefix gives.
    auto eager = read(90);
    ASSERT_EQ(eager.size(), 2);
    EXPECT_EQ(eager[1], _tablet->_stale_rs_version_map.at(Version(2, 90)));
    EXPECT_EQ(eager[1]->num_rows(), rowsets[1]->num_rows());

    std::vector<RowSetSplits> splits;
    ASSERT_TRUE(_tablet->capture_rs_readers(Version(0, 50), &splits, {}).ok());
    ASSERT_EQ(splits.size(), 2);
    EXPECT_EQ(splits[1].rs_reader->version(), Version(2, 50));
}

TEST_F(CloudHolePrefixTest, ConcurrentReadersShareOnePrefix) {
    make_range();
    constexpr int kReaders = 16;
    std::vector<RowsetSharedPtr> prefixes(kReaders);
    std::vector<std::thread> readers;
    for (int i = 0; i < kReaders; ++i) {
        readers.emplace_back([&, i] {
            auto rowsets = read(40);
            if (rowsets.size() == 2) {
                prefixes[i] = rowsets[1];
            }
        });
    }
    for (auto& reader : readers) {
        reader.join();
    }
    for (int i = 0; i < kReaders; ++i) {
        ASSERT_NE(prefixes[i], nullptr);
        EXPECT_EQ(prefixes[i], prefixes[0]);
    }
    std::weak_ptr<Rowset> held = prefixes[0];
    prefixes.clear();
    // Freed with its last reader; the next read makes a new one.
    EXPECT_TRUE(held.expired());
    auto again = read(40);
    ASSERT_EQ(again.size(), 2);
    EXPECT_EQ(again[1]->version(), Version(2, 40));
}

TEST_F(CloudHolePrefixTest, IncrementalReadStartingAtRange) {
    make_range();
    std::shared_lock rlock(_tablet->get_header_lock());
    auto captured = _tablet->capture_consistent_rowsets_unlocked(Version(2, 30), {});
    ASSERT_TRUE(captured.has_value());
    ASSERT_EQ(captured.value().rowsets.size(), 1);
    EXPECT_EQ(captured.value().rowsets[0]->version(), Version(2, 30));
}

TEST_F(CloudHolePrefixTest, CompactionOverHoleRangesKeepsVersionsReadable) {
    config::hole_rowset_eager_prefix_versions = 0;
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1)), create_rowset(Version(5, 5))}, false);
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 12).ok());
    ASSERT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 4}, {5, 5}, {6, 12}}));
    {
        // What a cumulative compaction of [2, 12] commits: inputs to the stale path, the output
        // in their place.
        std::unique_lock wlock(_tablet->get_header_lock());
        std::vector<RowsetSharedPtr> inputs;
        for (const auto& version : {Version(2, 4), Version(5, 5), Version(6, 12)}) {
            inputs.push_back(_tablet->get_rowset_by_version(version));
        }
        _tablet->delete_rowsets(inputs, wlock);
        _tablet->add_rowsets({create_rowset(Version(2, 12))}, false, wlock, false);
    }
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 12}}));
    // Versions inside the output stay readable through the stale inputs while those last.
    for (int64_t v : {3, 4, 5, 8, 12}) {
        auto rowsets = read(v);
        ASSERT_FALSE(rowsets.empty()) << v;
        EXPECT_EQ(rowsets.back()->version().second, v) << v;
    }
    EXPECT_EQ(read(3).back()->version(), Version(2, 3));
    EXPECT_EQ(read(8).back()->version(), Version(6, 8));
}

TEST_F(CloudHolePrefixTest, DisabledRangesNeedNoPrefixes) {
    config::enable_hole_rowset_version_range = false;
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 6).ok());
    EXPECT_EQ(live_versions(*_tablet),
              (VersionSet {{0, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}}));
    std::shared_lock rlock(_tablet->get_header_lock());
    for (int64_t v = 3; v <= 6; ++v) {
        EXPECT_EQ(_tablet->on_demand_rowset_unlocked(Version(2, v)), nullptr) << v;
    }
    expect_all_versions_capturable(*_tablet);
}

} // namespace doris

namespace doris {

// Implicit version gaps: version holes the meta-service confirmed empty are kept as version graph
// edges only, with no hole rowset.
class CloudImplicitGapTest : public CloudHoleRowsetRangeTest {
public:
    void SetUp() override {
        CloudHoleRowsetRangeTest::SetUp();
        _saved_gaps = config::enable_implicit_version_gaps;
        config::enable_implicit_version_gaps = true;
        config::hole_rowset_eager_prefix_versions = 0;
    }
    void TearDown() override {
        config::enable_implicit_version_gaps = _saved_gaps;
        CloudHoleRowsetRangeTest::TearDown();
    }

    Status sync_fill(CloudTablet* tablet, int64_t max_version, bool resolved = true) {
        std::unique_lock wlock(tablet->get_header_lock());
        return _engine.meta_mgr().fill_version_holes(tablet, max_version, wlock, resolved);
    }

    static std::map<int64_t, int64_t> gaps(const CloudTablet& tablet) {
        return tablet._gap_versions;
    }

    static int64_t exposed(const std::string& name) {
        return std::stoll(bvar::Variable::describe_exposed(name));
    }

    // Compaction candidates as a picker sees them: rowsets and gaps, in version order.
    std::vector<RowsetSharedPtr> candidates(CloudTablet* tablet, int64_t from, int64_t to) {
        std::vector<RowsetSharedPtr> out;
        std::shared_lock rlock(tablet->get_header_lock());
        tablet->traverse_rowsets_and_gaps_unlocked([&](const RowsetSharedPtr& rs) {
            if (rs->start_version() >= from && rs->end_version() <= to) {
                out.push_back(rs);
            }
        });
        std::sort(out.begin(), out.end(), Rowset::comparator);
        return out;
    }

    // What a compaction commits locally once the meta-service accepted it.
    void commit_compaction(CloudTablet* tablet, const std::vector<RowsetSharedPtr>& inputs) {
        std::unique_lock wlock(tablet->get_header_lock());
        tablet->delete_rowsets(inputs, wlock);
        tablet->add_rowsets({create_rowset(Version(inputs.front()->start_version(),
                                                   inputs.back()->end_version()))},
                            false, wlock, false);
    }

    // Data rowsets a read of [0, version] returns.
    std::vector<RowsetId> data_read(CloudTablet* tablet, int64_t version) {
        std::vector<RowsetId> ids;
        std::shared_lock rlock(tablet->get_header_lock());
        auto captured = tablet->capture_consistent_rowsets_unlocked(Version(0, version), {});
        EXPECT_TRUE(captured.has_value()) << "version " << version;
        if (captured.has_value()) {
            for (const auto& rs : captured.value().rowsets) {
                if (!rs->is_hole_rowset()) {
                    ids.push_back(rs->rowset_id());
                }
            }
        }
        std::sort(ids.begin(), ids.end());
        return ids;
    }

protected:
    bool _saved_gaps = false;
};

TEST_F(CloudImplicitGapTest, ConfirmedHolesBecomeGapsWithoutRowsets) {
    const int64_t filled_before = exposed("implicit_gap_filled_count");
    add_rowsets(_tablet.get(),
                {create_rowset(Version(0, 1)), create_rowset(Version(5, 5)),
                 create_rowset(Version(9, 12))},
                false);
    ASSERT_TRUE(sync_fill(_tablet.get(), 20).ok());
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {5, 5}, {9, 12}}));
    EXPECT_EQ(gaps(*_tablet), (std::map<int64_t, int64_t> {{2, 4}, {6, 8}, {13, 20}}));
    EXPECT_EQ(_tablet->tablet_meta()->all_rs_metas().size(), 3);
    EXPECT_TRUE(_tablet->_stale_rs_version_map.empty());
    EXPECT_EQ(_tablet->max_version_unlocked(), 20);
    EXPECT_EQ(exposed("implicit_gap_filled_count") - filled_before, 3 + 3 + 8);
    expect_all_versions_capturable(*_tablet);

    // A newer partition version extends the trailing gap; nothing is refilled.
    ASSERT_TRUE(sync_fill(_tablet.get(), 25).ok());
    EXPECT_EQ(gaps(*_tablet), (std::map<int64_t, int64_t> {{2, 4}, {6, 8}, {13, 25}}));
    EXPECT_EQ(exposed("implicit_gap_filled_count") - filled_before, 3 + 3 + 8 + 5);
    expect_all_versions_capturable(*_tablet);

    // A read inside a gap gets an empty transient rowset; the tablet keeps none.
    std::shared_lock rlock(_tablet->get_header_lock());
    auto captured = _tablet->capture_consistent_rowsets_unlocked(Version(0, 17), {});
    ASSERT_TRUE(captured.has_value()) << captured.error();
    ASSERT_EQ(captured.value().rowsets.back()->version(), Version(13, 17));
    EXPECT_TRUE(captured.value().rowsets.back()->is_hole_rowset());
    EXPECT_EQ(captured.value().rowsets.back()->num_segments(), 0);
    EXPECT_TRUE(_tablet->_stale_rs_version_map.empty());
}

TEST_F(CloudImplicitGapTest, UnconfirmedHolesKeepHoleRowsets) {
    const int64_t unsafe_before = exposed("implicit_gap_disabled_unsafe_ms");
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1)), create_rowset(Version(5, 5))}, false);
    ASSERT_TRUE(sync_fill(_tablet.get(), 10, /*resolved=*/false).ok());
    EXPECT_TRUE(gaps(*_tablet).empty());
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 4}, {5, 5}, {6, 10}}));
    EXPECT_EQ(exposed("implicit_gap_disabled_unsafe_ms") - unsafe_before, 1);
    expect_all_versions_capturable(*_tablet);
}

TEST_F(CloudImplicitGapTest, MergeOnWriteAndDisabledKeepHoleRowsets) {
    auto mow = make_tablet(true);
    add_rowsets(mow.get(), {create_rowset(Version(0, 1))}, false);
    ASSERT_TRUE(sync_fill(mow.get(), 4).ok());
    EXPECT_TRUE(gaps(*mow).empty());
    EXPECT_EQ(live_versions(*mow), (VersionSet {{0, 1}, {2, 2}, {3, 3}, {4, 4}}));

    config::enable_implicit_version_gaps = false;
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    ASSERT_TRUE(sync_fill(_tablet.get(), 4).ok());
    EXPECT_TRUE(gaps(*_tablet).empty());
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 4}}));
}

TEST_F(CloudImplicitGapTest, CompactionCrossesGapsAndKeepsVersionsReadable) {
    add_rowsets(_tablet.get(),
                {create_rowset(Version(0, 1)), create_rowset(Version(5, 5)),
                 create_rowset(Version(9, 12))},
                false);
    ASSERT_TRUE(sync_fill(_tablet.get(), 20).ok());
    auto inputs = candidates(_tablet.get(), 2, 12);
    std::vector<Version> versions;
    for (const auto& rs : inputs) {
        versions.push_back(rs->version());
    }
    ASSERT_EQ(versions,
              (std::vector<Version> {Version(2, 4), Version(5, 5), Version(6, 8), Version(9, 12)}));
    EXPECT_TRUE(inputs[0]->is_hole_rowset());
    EXPECT_TRUE(inputs[2]->is_hole_rowset());
    ASSERT_TRUE(_tablet->check_gap_inputs(inputs).ok());

    commit_compaction(_tablet.get(), inputs);
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 12}}));
    EXPECT_EQ(gaps(*_tablet), (std::map<int64_t, int64_t> {{13, 20}}));
    expect_all_versions_capturable(*_tablet);
    // Versions inside the output stay readable through the stale inputs while those last.
    for (int64_t v : {4, 5, 8}) {
        std::shared_lock rlock(_tablet->get_header_lock());
        auto captured = _tablet->capture_consistent_rowsets_unlocked(Version(0, v), {});
        ASSERT_TRUE(captured.has_value()) << v;
        EXPECT_EQ(captured.value().rowsets.back()->version().second, v) << v;
    }

    // Expiring the stale path and rebuilding the version graph keeps the remaining gap.
    const auto saved_sweep = config::tablet_rowset_stale_sweep_time_sec;
    const auto saved_ratio = config::tablet_version_graph_orphan_vertex_ratio;
    config::tablet_rowset_stale_sweep_time_sec = -1000;
    config::tablet_version_graph_orphan_vertex_ratio = 0;
    EXPECT_GT(_tablet->delete_expired_stale_rowsets(), 0);
    config::tablet_rowset_stale_sweep_time_sec = saved_sweep;
    config::tablet_version_graph_orphan_vertex_ratio = saved_ratio;
    EXPECT_TRUE(_tablet->_stale_rs_version_map.empty());
    EXPECT_EQ(gaps(*_tablet), (std::map<int64_t, int64_t> {{13, 20}}));
    expect_all_versions_capturable(*_tablet);
}

TEST_F(CloudImplicitGapTest, CompactionHoldingAnExtendedGapStillCommits) {
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1)), create_rowset(Version(2, 2))}, false);
    ASSERT_TRUE(sync_fill(_tablet.get(), 10).ok());
    auto inputs = candidates(_tablet.get(), 2, 10);
    ASSERT_EQ(inputs.size(), 2);
    ASSERT_EQ(inputs[1]->version(), Version(3, 10));
    // A sync extends the gap while the compaction runs.
    ASSERT_TRUE(sync_fill(_tablet.get(), 15).ok());
    ASSERT_EQ(gaps(*_tablet), (std::map<int64_t, int64_t> {{3, 15}}));
    ASSERT_TRUE(_tablet->check_gap_inputs(inputs).ok());
    commit_compaction(_tablet.get(), inputs);
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 10}}));
    EXPECT_EQ(gaps(*_tablet), (std::map<int64_t, int64_t> {{11, 15}}));
    expect_all_versions_capturable(*_tablet);
}

TEST_F(CloudImplicitGapTest, LoadInsideAGapRefusesTheCompaction) {
    const int64_t refused_before = exposed("compaction_implicit_gap_refused");
    const int64_t overlap_before = exposed("implicit_gap_load_overlap");
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1)), create_rowset(Version(2, 2))}, false);
    ASSERT_TRUE(sync_fill(_tablet.get(), 10).ok());
    auto inputs = candidates(_tablet.get(), 2, 10);
    ASSERT_EQ(inputs.size(), 2);
    // A rowset at version 6 arrives, inside the gap the compaction picked.
    auto loaded = create_rowset(Version(6, 6));
    add_rowsets(_tablet.get(), {loaded}, true);
    EXPECT_EQ(exposed("implicit_gap_load_overlap") - overlap_before, 1);
    EXPECT_EQ(gaps(*_tablet), (std::map<int64_t, int64_t> {{3, 5}, {7, 10}}));
    auto st = _tablet->check_gap_inputs(inputs);
    EXPECT_TRUE(st.is<ErrorCode::INPUT_GAP_UNSAFE>()) << st;
    EXPECT_EQ(exposed("compaction_implicit_gap_refused") - refused_before, 1);
    // The data is intact and every version readable.
    EXPECT_TRUE(live_versions(*_tablet).contains({6, 6}));
    expect_all_versions_capturable(*_tablet);
    const auto read = data_read(_tablet.get(), 10);
    EXPECT_TRUE(std::find(read.begin(), read.end(), loaded->rowset_id()) != read.end());
}

TEST_F(CloudImplicitGapTest, TurningGapsOffMaterializesThem) {
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1)), create_rowset(Version(5, 5))}, false);
    ASSERT_TRUE(sync_fill(_tablet.get(), 12).ok());
    ASSERT_EQ(gaps(*_tablet), (std::map<int64_t, int64_t> {{2, 4}, {6, 12}}));
    config::enable_implicit_version_gaps = false;
    ASSERT_TRUE(sync_fill(_tablet.get(), 14).ok());
    EXPECT_TRUE(gaps(*_tablet).empty());
    // The sync's new versions [13, 14] extend the materialized trailing hole rowset.
    for (const auto& v : {Version(2, 4), Version(6, 14)}) {
        ASSERT_TRUE(_tablet->rowset_map().contains(v)) << v.to_string();
        EXPECT_TRUE(_tablet->rowset_map().at(v)->is_hole_rowset());
    }
    expect_all_versions_capturable(*_tablet);
}

// Loads at random versions and syncs, on one tablet with gaps and one with hole rowsets: every
// version reads the same data rowsets on both, and compacting the gap tablet keeps every version
// readable.
TEST_F(CloudImplicitGapTest, RandomLoadsReadTheSameAsHoleRowsets) {
    std::mt19937 rng(20261021);
    for (int round = 0; round < 20; ++round) {
        auto with_gaps = make_tablet(false);
        auto with_holes = make_tablet(false);
        auto base = create_rowset(Version(0, 1));
        add_rowsets(with_gaps.get(), {base}, false);
        add_rowsets(with_holes.get(), {base}, false);
        int64_t partition_version = 1;
        for (int step = 0; step < 30; ++step) {
            partition_version += 1 + static_cast<int64_t>(rng() % 6);
            if (rng() % 2 == 0) {
                // This tablet got the newest load; every version before it went to other tablets.
                auto rs = create_rowset(Version(partition_version, partition_version));
                for (auto* tablet : {with_gaps.get(), with_holes.get()}) {
                    std::shared_lock rlock(tablet->get_header_lock());
                    const bool overlap = tablet->max_version_unlocked() >= partition_version;
                    rlock.unlock();
                    add_rowsets(tablet, {rs}, overlap);
                }
            }
            ASSERT_TRUE(sync_fill(with_gaps.get(), partition_version).ok());
            config::enable_implicit_version_gaps = false;
            ASSERT_TRUE(sync_fill(with_holes.get(), partition_version).ok());
            config::enable_implicit_version_gaps = true;
        }
        ASSERT_TRUE(with_holes->_gap_versions.empty());
        for (int64_t v = 1; v <= partition_version; ++v) {
            ASSERT_EQ(data_read(with_gaps.get(), v), data_read(with_holes.get(), v))
                    << "round " << round << " version " << v;
        }
        expect_all_versions_capturable(*with_gaps);
        // A compaction over a random run of candidates.
        auto all = candidates(with_gaps.get(), 2, partition_version);
        if (all.size() >= 2) {
            size_t first = rng() % (all.size() - 1);
            size_t last = first + 1 + rng() % (all.size() - first - 1);
            std::vector<RowsetSharedPtr> inputs(all.begin() + first, all.begin() + last + 1);
            ASSERT_TRUE(with_gaps->check_gap_inputs(inputs).ok());
            commit_compaction(with_gaps.get(), inputs);
            expect_all_versions_capturable(*with_gaps);
        }
    }
}

} // namespace doris
