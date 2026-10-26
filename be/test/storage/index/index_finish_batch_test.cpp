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

#include "storage/index/index_finish_batch.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "common/config.h"
#include "storage/index/index_writer.h"

namespace doris::segment_v2 {
namespace {

class FakeIndexWriter final : public IndexColumnWriter {
public:
    explicit FakeIndexWriter(int64_t size, Status result = Status::OK())
            : _size(size), _result(std::move(result)) {}

    Status init() override { return Status::OK(); }
    Status add_values(const std::string, const void*, size_t) override { return Status::OK(); }
    Status add_array_values(size_t, const void*, const uint8_t*, const uint8_t*, size_t) override {
        return Status::OK();
    }
    Status add_nulls(uint32_t) override { return Status::OK(); }
    Status add_array_nulls(const uint8_t*, size_t) override { return Status::OK(); }
    Status finish() override {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        ++finished;
        std::lock_guard lock(mutex);
        threads.insert(std::this_thread::get_id());
        return _result;
    }
    int64_t size() const override { return _size; }
    void close_on_error() override {}

    std::atomic<int> finished {0};
    static std::mutex mutex;
    static std::set<std::thread::id> threads;

private:
    int64_t _size;
    Status _result;
};

std::mutex FakeIndexWriter::mutex;
std::set<std::thread::id> FakeIndexWriter::threads;

class ParallelismConfig {
public:
    explicit ParallelismConfig(int32_t parallelism)
            : _old(config::index_finish_parallelism),
              _old_min_bytes(config::segment_write_task_min_bytes) {
        config::index_finish_parallelism = parallelism;
        config::segment_write_task_min_bytes = 0;
    }
    ~ParallelismConfig() {
        config::index_finish_parallelism = _old;
        config::segment_write_task_min_bytes = _old_min_bytes;
    }

private:
    int32_t _old;
    int64_t _old_min_bytes;
};

std::vector<std::unique_ptr<FakeIndexWriter>> make_writers(int n) {
    std::vector<std::unique_ptr<FakeIndexWriter>> writers;
    for (int i = 0; i < n; ++i) {
        writers.push_back(std::make_unique<FakeIndexWriter>(i));
    }
    return writers;
}

} // namespace

// Writers queued while a batch is open finish when it runs, each once, on several threads.
TEST(IndexFinishBatchTest, QueuedWritersFinishOnceInParallel) {
    ParallelismConfig parallelism(4);
    auto writers = make_writers(12);
    FakeIndexWriter::threads.clear();
    auto start = std::chrono::steady_clock::now();
    {
        IndexFinishBatch batch;
        for (auto& w : writers) {
            ASSERT_TRUE(IndexFinishBatch::finish_index(w.get()).ok());
            EXPECT_EQ(w->finished, 0);
        }
        ASSERT_TRUE(batch.run().ok());
    }
    const auto elapsed = std::chrono::steady_clock::now() - start;
    for (auto& w : writers) {
        EXPECT_EQ(w->finished, 1);
    }
    EXPECT_GT(FakeIndexWriter::threads.size(), 1);
    // 12 finishes of 20 ms on 4 threads.
    EXPECT_LT(elapsed, std::chrono::milliseconds(12 * 20));
}

// The first error is returned, and the other writers still finish.
TEST(IndexFinishBatchTest, ErrorIsReturnedAndOthersFinish) {
    ParallelismConfig parallelism(4);
    auto writers = make_writers(6);
    FakeIndexWriter failing(100, Status::IOError("injected"));
    IndexFinishBatch batch;
    for (auto& w : writers) {
        ASSERT_TRUE(IndexFinishBatch::finish_index(w.get()).ok());
    }
    ASSERT_TRUE(IndexFinishBatch::finish_index(&failing).ok());
    auto st = batch.run();
    EXPECT_TRUE(st.is<ErrorCode::IO_ERROR>()) << st;
    for (auto& w : writers) {
        EXPECT_EQ(w->finished, 1);
    }
    EXPECT_EQ(failing.finished, 1);
}

// Without a batch, or with parallelism 1, a writer finishes at once on the calling thread; a
// batch that is not run finishes its writers when it is destroyed.
TEST(IndexFinishBatchTest, FinishesInlineWithoutABatch) {
    FakeIndexWriter outside(1);
    ASSERT_TRUE(IndexFinishBatch::finish_index(&outside).ok());
    EXPECT_EQ(outside.finished, 1);
    {
        ParallelismConfig parallelism(1);
        IndexFinishBatch batch;
        FakeIndexWriter serial(1);
        ASSERT_TRUE(IndexFinishBatch::finish_index(&serial).ok());
        EXPECT_EQ(serial.finished, 1);
        ASSERT_TRUE(batch.run().ok());
    }
    ParallelismConfig parallelism(4);
    auto writers = make_writers(3);
    {
        IndexFinishBatch batch;
        for (auto& w : writers) {
            ASSERT_TRUE(IndexFinishBatch::finish_index(w.get()).ok());
        }
    }
    for (auto& w : writers) {
        EXPECT_EQ(w->finished, 1);
    }
    FakeIndexWriter after(1);
    ASSERT_TRUE(IndexFinishBatch::finish_index(&after).ok());
    EXPECT_EQ(after.finished, 1);
}

} // namespace doris::segment_v2
