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
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <set>
#include <string>
#include <thread>
#include <chrono>
#include <optional>
#include <vector>
#include <bvar/bvar.h>

#include "common/config.h"
#include "cpp/sync_point.h"
#include "runtime/exec_env.h"
#include "storage/index/index_file_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/index/index_writer.h"
#include "util/defer_op.h"

namespace doris::segment_v2 {
namespace {

namespace fs = std::filesystem;

class TmpFileDirsTest : public testing::Test {
protected:
    void SetUp() override {
        _root = fs::absolute("ut_dir/tmp_file_dirs_test").lexically_normal();
        fs::remove_all(_root);
        fs::create_directories(_root / "store");
        fs::create_directories(_root / "cache1");
        fs::create_directories(_root / "cache2");
        _saved_dirs = config::index_tmp_file_dirs;
        _saved_min_free = config::index_tmp_file_dir_min_free_bytes;
        _saved_cache_path = config::file_cache_path;
        config::file_cache_path = R"([{"path":")" + (_root / "cache1").native() +
                                  R"(","total_size":1048576},{"path":")" +
                                  (_root / "cache2").native() + R"(","total_size":1048576}])";
    }
    void TearDown() override {
        config::index_tmp_file_dirs = _saved_dirs;
        config::index_tmp_file_dir_min_free_bytes = _saved_min_free;
        config::file_cache_path = _saved_cache_path;
        fs::remove_all(_root);
    }

    std::vector<StorePath> store_paths() const {
        return {StorePath((_root / "store").native(), -1)};
    }

    Status validate(const std::string& dir) const {
        return TmpFileDirs::validate_index_tmp_dir(
                dir, {(_root / "store").native()},
                {(_root / "cache1").native(), (_root / "cache2").native()},
                {(_root / "log").native(), (_root / "conf").native()});
    }

    fs::path _root;
    std::string _saved_dirs;
    int64_t _saved_min_free = 0;
    std::string _saved_cache_path;
};

TEST_F(TmpFileDirsTest, EmptyConfigUsesTheStorePathsTmpDirs) {
    config::index_tmp_file_dirs = "";
    TmpFileDirs dirs(store_paths());
    ASSERT_TRUE(dirs.init().ok());
    EXPECT_TRUE(dirs.index_tmp_dirs().empty());
    EXPECT_EQ(dirs.get_tmp_file_dir(), _root / "store" / config::tmp_file_dir);
    EXPECT_TRUE(fs::is_directory(_root / "store" / config::tmp_file_dir));
}

TEST_F(TmpFileDirsTest, ConfiguredDirsAreUsedRoundRobin) {
    const auto a = _root / "cache1" / "index_tmp";
    const auto b = _root / "cache2" / "index_tmp";
    config::index_tmp_file_dirs = a.native() + ", " + b.native();
    config::index_tmp_file_dir_min_free_bytes = 0;
    fs::create_directories(a / "stale");
    TmpFileDirs dirs(store_paths());
    ASSERT_TRUE(dirs.init().ok());
    ASSERT_EQ(dirs.index_tmp_dirs().size(), 2);
    EXPECT_FALSE(fs::exists(a / "stale"));
    std::vector<io::Path> picks;
    for (int i = 0; i < 4; ++i) {
        picks.push_back(dirs.get_tmp_file_dir());
    }
    EXPECT_EQ(picks[0], picks[2]);
    EXPECT_EQ(picks[1], picks[3]);
    EXPECT_NE(picks[0], picks[1]);
    EXPECT_EQ(std::set<io::Path>(picks.begin(), picks.end()), (std::set<io::Path> {a, b}));
}

TEST_F(TmpFileDirsTest, UnsafeDirsAreRefused) {
    EXPECT_TRUE(validate((_root / "cache1" / "index_tmp").native()).ok());
    EXPECT_TRUE(validate((_root / "elsewhere" / "index_tmp").native()).ok());
    // relative
    EXPECT_FALSE(validate("relative/index_tmp").ok());
    EXPECT_FALSE(validate("/").ok());
    // a storage root, or a dir containing one
    EXPECT_FALSE(validate((_root / "store").native()).ok());
    EXPECT_FALSE(validate(_root.native()).ok());
    // a cache path, a dir containing one, or anything else inside one
    EXPECT_FALSE(validate((_root / "cache1").native()).ok());
    EXPECT_FALSE(validate((_root / "cache1" / "other").native()).ok());
    EXPECT_FALSE(validate((_root / "cache1" / "abc" / "index_tmp").native()).ok());
    // log and conf dirs
    EXPECT_FALSE(validate((_root / "log").native()).ok());
    EXPECT_FALSE(validate((_root / "log" / "index_tmp").native()).ok());
    EXPECT_FALSE(validate((_root / "conf").native()).ok());
    // a mount point
    if (fs::exists("/proc/self")) {
        EXPECT_FALSE(TmpFileDirs::validate_index_tmp_dir("/proc", {}, {}, {}).ok());
    }
}

TEST_F(TmpFileDirsTest, InitRefusesAnUnsafeDirBeforeDeletingAnything) {
    const auto good = _root / "cache1" / "index_tmp";
    fs::create_directories(good / "keep");
    std::ofstream(_root / "cache1" / "data") << "cache";
    config::index_tmp_file_dirs = good.native() + "," + (_root / "cache2").native();
    TmpFileDirs dirs(store_paths());
    EXPECT_FALSE(dirs.init().ok());
    EXPECT_TRUE(fs::exists(good / "keep"));
    EXPECT_TRUE(fs::exists(_root / "cache1" / "data"));
}

TEST_F(TmpFileDirsTest, InitFailsOnAnUnwritableDir) {
    if (::geteuid() == 0) {
        GTEST_SKIP() << "root can write anywhere";
    }
    const auto parent = _root / "cache1";
    config::index_tmp_file_dirs = (parent / "index_tmp").native();
    ::chmod(parent.c_str(), 0500);
    Defer restore {[&]() { ::chmod(parent.c_str(), 0755); }};
    TmpFileDirs dirs(store_paths());
    EXPECT_FALSE(dirs.init().ok());
}

TEST_F(TmpFileDirsTest, DirsLowOnSpaceArePassedOver) {
    const auto a = _root / "cache1" / "index_tmp";
    const auto b = _root / "cache2" / "index_tmp";
    config::index_tmp_file_dirs = a.native() + "," + b.native();
    config::index_tmp_file_dir_min_free_bytes = 100;
    TmpFileDirs dirs(store_paths());
    ASSERT_TRUE(dirs.init().ok());
    int64_t free_a = 1000;
    int64_t free_b = 1000;
    dirs.set_free_bytes_function_for_test(
            [&](const io::Path& dir) { return dir == a ? free_a : free_b; });
    free_a = 10;
    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(dirs.get_tmp_file_dir(), b);
    }
    free_b = 10;
    // Free space is re-read at most once a second.
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_EQ(dirs.get_tmp_file_dir(), _root / "store" / config::tmp_file_dir);
}

void age(const fs::path& path, std::chrono::minutes by) {
    const auto when = fs::file_time_type::clock::now() - by;
    if (fs::is_directory(path)) {
        for (const auto& entry : fs::recursive_directory_iterator(path)) {
            fs::last_write_time(entry.path(), when);
        }
    }
    fs::last_write_time(path, when);
}

int64_t exposed(const std::string& name) {
    return std::stoll(bvar::Variable::describe_exposed(name));
}

// The sweeper removes an aged entry no writer owns, and keeps young ones, registered ones and
// symbolic links, whose targets it does not follow.
TEST_F(TmpFileDirsTest, SweeperRemovesOnlyAgedUnownedEntries) {
    config::index_tmp_file_dirs = "";
    const int saved_age = config::index_tmp_orphan_max_age_minutes;
    config::index_tmp_orphan_max_age_minutes = 60;
    Defer restore {[&]() { config::index_tmp_orphan_max_age_minutes = saved_age; }};
    TmpFileDirs dirs(store_paths());
    ASSERT_TRUE(dirs.init().ok());
    EXPECT_EQ(dirs.sweep_orphans(), 0);
    const fs::path tmp = _root / "store" / config::tmp_file_dir;
    for (const char* name : {"orphan_0_1@a", "young_0_1", "owned_0_1"}) {
        fs::create_directories(tmp / name);
        std::ofstream(tmp / name / "_0.frq") << "postings";
    }
    fs::create_directories(_root / "outside");
    std::ofstream(_root / "outside" / "keep") << "keep";
    fs::create_directory_symlink(_root / "outside", tmp / "link");
    age(tmp / "orphan_0_1@a", std::chrono::minutes(120));
    age(tmp / "owned_0_1", std::chrono::minutes(120));
    ScopedIndexTmpPrefix owned("owned_0_");
    const int64_t removed_before = exposed("index_tmp_orphan_dirs_removed");
    const int64_t bytes_before = exposed("index_tmp_orphan_bytes_removed");
    EXPECT_EQ(dirs.sweep_orphans(), 1);
    EXPECT_FALSE(fs::exists(tmp / "orphan_0_1@a"));
    EXPECT_TRUE(fs::exists(tmp / "young_0_1"));
    EXPECT_TRUE(fs::exists(tmp / "owned_0_1"));
    EXPECT_TRUE(fs::is_symlink(tmp / "link"));
    EXPECT_TRUE(fs::exists(_root / "outside" / "keep"));
    EXPECT_EQ(exposed("index_tmp_orphan_dirs_removed") - removed_before, 1);
    EXPECT_EQ(exposed("index_tmp_orphan_bytes_removed") - bytes_before, 8);
}

// An entry whose writer registers after the sweeper listed the dir is not removed.
TEST_F(TmpFileDirsTest, SweeperKeepsAnEntryRegisteredAfterListing) {
    config::index_tmp_file_dirs = "";
    const int saved_age = config::index_tmp_orphan_max_age_minutes;
    config::index_tmp_orphan_max_age_minutes = 60;
    Defer restore {[&]() { config::index_tmp_orphan_max_age_minutes = saved_age; }};
    TmpFileDirs dirs(store_paths());
    ASSERT_TRUE(dirs.init().ok());
    const fs::path tmp = _root / "store" / config::tmp_file_dir;
    fs::create_directories(tmp / "late_0_1");
    age(tmp / "late_0_1", std::chrono::minutes(120));
    std::optional<ScopedIndexTmpPrefix> late;
    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            "TmpFileDirs::sweep_orphans:listed", [&](auto&&) { late.emplace("late_0_"); },
            &guard);
    EXPECT_EQ(dirs.sweep_orphans(), 0);
    sp->disable_processing();
    EXPECT_TRUE(fs::exists(tmp / "late_0_1"));
}

// A writer destroyed without closing, as when its load or compaction failed, deletes the index
// dirs it opened, and no longer owns its prefix.
TEST_F(TmpFileDirsTest, UnclosedIndexFileWriterLeavesNothing) {
    config::index_tmp_file_dirs = "";
    auto owned = std::make_unique<TmpFileDirs>(store_paths());
    ASSERT_TRUE(owned->init().ok());
    ExecEnv::GetInstance()->set_tmp_file_dir(std::move(owned));
    const fs::path tmp = _root / "store" / config::tmp_file_dir;
    TabletIndexPB index_pb;
    index_pb.set_index_id(7);
    index_pb.set_index_name("idx");
    index_pb.set_index_type(IndexType::INVERTED);
    index_pb.add_col_unique_id(1);
    TabletIndex index_meta;
    index_meta.init_from_pb(index_pb);
    {
        IndexFileWriter writer(io::global_local_filesystem(), (_root / "segment_0").native(),
                               "unclosed", 0, InvertedIndexStorageFormatPB::V2, nullptr,
                               false /* can_use_ram_dir */);
        EXPECT_TRUE(IndexTmpRegistry::instance().covers("unclosed_0_7"));
        auto dir = writer.open(&index_meta);
        ASSERT_TRUE(dir.has_value()) << dir.error();
        std::ofstream(tmp / "unclosed_0_7" / "_0.frq") << "postings";
        EXPECT_TRUE(fs::exists(tmp / "unclosed_0_7"));
    }
    EXPECT_FALSE(fs::exists(tmp / "unclosed_0_7"));
    EXPECT_FALSE(IndexTmpRegistry::instance().covers("unclosed_0_7"));
}

} // namespace
} // namespace doris::segment_v2
