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

#include "load/memtable/memtable_flush_executor.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>
#include <sys/file.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

#include "common/config.h"
#include "io/fs/local_file_system.h"
#include "load/delta_writer/delta_writer.h"
#include "load/memtable/memtable.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "core/pod_array.h"
#include "storage/options.h"
#include "storage/schema.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta_manager.h"
#include "storage/utils.h"

namespace doris {

void set_up() {
    char buffer[1024];
    getcwd(buffer, 1024);
    config::storage_root_path = std::string(buffer) + "/flush_test";
    auto st = io::global_local_filesystem()->delete_directory(config::storage_root_path);
    ASSERT_TRUE(st.ok()) << st;
    st = io::global_local_filesystem()->create_directory(config::storage_root_path);
    ASSERT_TRUE(st.ok()) << st;

    std::vector<StorePath> paths;
    paths.emplace_back(config::storage_root_path, -1);

    doris::EngineOptions options;
    options.store_paths = paths;
    auto engine = std::make_unique<StorageEngine>(options);
    Status s = engine->open();
    EXPECT_TRUE(s.ok()) << s.to_string();
    ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
}

void tear_down() {
    ExecEnv::GetInstance()->set_storage_engine(nullptr);
    system("rm -rf ./flush_test");
    EXPECT_TRUE(io::global_local_filesystem()
                        ->delete_directory(std::string(getenv("DORIS_HOME")) + "/" + UNUSED_PREFIX)
                        .ok());
}

TEST(MemTableFlushExecutorTest, TestDynamicThreadPoolUpdate) {
    // Setup
    set_up();

    auto* flush_executor = ExecEnv::GetInstance()->storage_engine().memtable_flush_executor();
    ASSERT_NE(flush_executor, nullptr);

    // Store original config values
    int32_t original_flush_thread_num = config::flush_thread_num_per_store;
    int32_t original_high_priority_flush_thread_num =
            config::high_priority_flush_thread_num_per_store;
    int32_t original_max_flush_thread_num = config::max_flush_thread_num_per_cpu;
    bool original_adaptive = config::enable_adaptive_flush_threads;

    // Test 1: Get initial thread pool sizes
    int initial_max_threads = flush_executor->flush_pool()->max_threads();
    int initial_min_threads = flush_executor->flush_pool()->min_threads();
    EXPECT_GT(initial_max_threads, 0);
    EXPECT_GT(initial_min_threads, 0);

    // Disable adaptive mode so flush_thread_num_per_store takes effect
    config::enable_adaptive_flush_threads = false;

    // Test 2: Update flush_thread_num_per_store and verify thread pool updates
    config::flush_thread_num_per_store = 10;
    flush_executor->update_memtable_flush_threads();

    int new_min_threads = flush_executor->flush_pool()->min_threads();
    EXPECT_EQ(new_min_threads, 10);

    // Test 3: Update max_flush_thread_num_per_cpu and verify thread pool updates
    config::max_flush_thread_num_per_cpu = 2;
    flush_executor->update_memtable_flush_threads();

    int num_cpus = std::thread::hardware_concurrency();
    if (num_cpus > 0) {
        int expected_max = std::min(10 * 1, num_cpus * 2); // 1 disk, 10 threads per store
        int actual_max = flush_executor->flush_pool()->max_threads();
        EXPECT_EQ(actual_max, expected_max);
    }

    // Test 4: Update high_priority_flush_thread_num_per_store
    config::high_priority_flush_thread_num_per_store = 8;
    flush_executor->update_memtable_flush_threads();
    // Note: We can't directly access _high_prio_flush_pool, but update should not crash

    // Test 5: Set very small values
    config::flush_thread_num_per_store = 0; // Should be adjusted to 1 by std::max
    flush_executor->update_memtable_flush_threads();
    EXPECT_GE(flush_executor->flush_pool()->min_threads(), 1);

    // Test 6: Set large values
    config::flush_thread_num_per_store = 100;
    flush_executor->update_memtable_flush_threads();
    EXPECT_GE(flush_executor->flush_pool()->min_threads(), 1);

    // Restore original config values
    config::flush_thread_num_per_store = original_flush_thread_num;
    config::high_priority_flush_thread_num_per_store = original_high_priority_flush_thread_num;
    config::max_flush_thread_num_per_cpu = original_max_flush_thread_num;
    config::enable_adaptive_flush_threads = original_adaptive;
    flush_executor->update_memtable_flush_threads();

    // Cleanup
    tear_down();
}

TEST(MemTableFlushExecutorTest, TestConfigUpdateTrigger) {
    // Setup
    set_up();

    auto* flush_executor = ExecEnv::GetInstance()->storage_engine().memtable_flush_executor();
    ASSERT_NE(flush_executor, nullptr);

    // Store original config values
    int32_t original_flush_thread_num = config::flush_thread_num_per_store;
    bool original_adaptive = config::enable_adaptive_flush_threads;

    // Disable adaptive mode so flush_thread_num_per_store takes effect
    config::enable_adaptive_flush_threads = false;
    flush_executor->update_memtable_flush_threads();

    // Get initial thread pool size
    int initial_min_threads = flush_executor->flush_pool()->min_threads();

    // Test: Simulate config update via set_config
    config::flush_thread_num_per_store = 15;
    config::update_config("flush_thread_num_per_store", "15");

    // Verify thread pool was updated
    int updated_min_threads = flush_executor->flush_pool()->min_threads();
    EXPECT_EQ(updated_min_threads, 15);
    EXPECT_NE(updated_min_threads, initial_min_threads);

    // Restore original config values
    config::flush_thread_num_per_store = original_flush_thread_num;
    config::enable_adaptive_flush_threads = original_adaptive;
    flush_executor->update_memtable_flush_threads();

    // Cleanup
    tear_down();
}

TEST(MemTableFlushExecutorTest, TestThreadPoolMinMaxRelationship) {
    // Setup
    set_up();

    auto* flush_executor = ExecEnv::GetInstance()->storage_engine().memtable_flush_executor();
    ASSERT_NE(flush_executor, nullptr);

    // Store original config values
    int32_t original_flush_thread_num = config::flush_thread_num_per_store;
    int32_t original_max_flush_thread_num = config::max_flush_thread_num_per_cpu;
    bool original_adaptive = config::enable_adaptive_flush_threads;

    // Disable adaptive mode so flush_thread_num_per_store takes effect
    config::enable_adaptive_flush_threads = false;

    // Test: Ensure min_threads <= max_threads always
    config::flush_thread_num_per_store = 20;
    config::max_flush_thread_num_per_cpu = 1; // Very restrictive
    flush_executor->update_memtable_flush_threads();

    int min_threads = flush_executor->flush_pool()->min_threads();
    int max_threads = flush_executor->flush_pool()->max_threads();
    EXPECT_LE(min_threads, max_threads);

    // Restore original config values
    config::flush_thread_num_per_store = original_flush_thread_num;
    config::max_flush_thread_num_per_cpu = original_max_flush_thread_num;
    config::enable_adaptive_flush_threads = original_adaptive;
    flush_executor->update_memtable_flush_threads();

    // Cleanup
    tear_down();
}

namespace {

class FlushLimitConfig {
public:
    FlushLimitConfig(int32_t limit, bool adaptive)
            : _limit(config::memtable_flush_running_count_limit),
              _adaptive(config::enable_adaptive_memtable_flush_running_count_limit) {
        config::memtable_flush_running_count_limit = limit;
        config::enable_adaptive_memtable_flush_running_count_limit = adaptive;
    }
    ~FlushLimitConfig() {
        config::memtable_flush_running_count_limit = _limit;
        config::enable_adaptive_memtable_flush_running_count_limit = _adaptive;
    }

private:
    int32_t _limit;
    bool _adaptive;
};

} // namespace

// A writer at its token's flush limit waits, and is woken as soon as one of the token's flushes
// ends, without polling.
TEST(MemTableFlushExecutorTest, FlushSlotWaitEndsWhenAFlushEnds) {
    FlushLimitConfig limit(2, false);
    auto token = FlushToken::create_shared(nullptr, nullptr);
    token->on_flush_started();
    token->on_flush_started();
    std::atomic<bool> done {false};
    std::thread writer([&] {
        token->wait_for_flush_slot(nullptr);
        done = true;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_FALSE(done);
    const auto ended = std::chrono::steady_clock::now();
    token->on_flush_finished();
    writer.join();
    EXPECT_TRUE(done);
    EXPECT_LT(std::chrono::steady_clock::now() - ended, std::chrono::milliseconds(80));
    token->on_flush_finished();
}

// A waiting writer stops waiting once it is cancelled, and a raised limit frees it.
TEST(MemTableFlushExecutorTest, FlushSlotWaitFollowsCancelAndLimit) {
    FlushLimitConfig limit(1, false);
    auto token = FlushToken::create_shared(nullptr, nullptr);
    token->on_flush_started();
    std::atomic<bool> cancelled {false};
    std::thread writer([&] { token->wait_for_flush_slot([&] { return cancelled.load(); }); });
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    cancelled = true;
    writer.join();

    std::atomic<bool> done {false};
    std::thread raised([&] {
        token->wait_for_flush_slot(nullptr);
        done = true;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_FALSE(done);
    config::memtable_flush_running_count_limit = 2;
    raised.join();
    EXPECT_TRUE(done);
    token->on_flush_finished();
}

// With the adaptive limit, each live token may run at least its share of the flush pool.
TEST(MemTableFlushExecutorTest, AdaptiveFlushLimitSharesThePool) {
    MemTableFlushExecutor executor;
    executor.init(1);
    const int threads = executor.flush_pool()->max_threads();
    auto token = FlushToken::create_shared(nullptr, nullptr);
    const int64_t tokens =
            std::stoll(bvar::Variable::describe_exposed("memtable_flush_live_tokens"));
    ASSERT_GE(tokens, 1);
    {
        FlushLimitConfig limit(2, false);
        EXPECT_EQ(FlushToken::flush_running_count_limit(), 2);
    }
    {
        FlushLimitConfig limit(2, true);
        EXPECT_EQ(FlushToken::flush_running_count_limit(),
                  std::max<int64_t>(2, threads / tokens));
    }
    {
        FlushLimitConfig limit(100000, true);
        EXPECT_EQ(FlushToken::flush_running_count_limit(), 100000);
    }
}

// A memtable's memory_usage(), which the memtable memory limiter sums for flushing memtables,
// counts what its flush allocates through Doris allocators under its tracker; memory allocated
// with plain new, as CLucene allocates index writer buffers, is not counted.
TEST(MemTableFlushExecutorTest, FlushAllocationsCountInTheMemtableTracker) {
    auto limiter = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::LOAD, "FlushMemory");
    SCOPED_ATTACH_TASK(limiter);
    auto memtable_tracker = std::make_shared<MemTracker>("MemTableFlush");
    {
        SCOPED_CONSUME_MEM_TRACKER(memtable_tracker);
        PaddedPODArray<char> block_data;
        block_data.resize(16 << 20);
        EXPECT_GE(memtable_tracker->consumption(), 16 << 20);
        const int64_t before = memtable_tracker->consumption();
        std::unique_ptr<char[]> untracked(new char[16 << 20]);
        untracked[0] = 1;
        EXPECT_LT(memtable_tracker->consumption() - before, 1 << 20);
    }
}

} // namespace doris
