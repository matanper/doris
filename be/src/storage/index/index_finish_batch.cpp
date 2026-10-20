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

#include <bvar/bvar.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <thread>

#include "common/config.h"
#include "common/logging.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "storage/index/index_writer.h"
#include "storage/segment/segment_write_tasks.h"
#include "util/countdown_latch.h"
#include "util/threadpool.h"

namespace doris::segment_v2 {
namespace {

thread_local IndexFinishBatch* t_batch = nullptr;

bvar::Adder<uint64_t> g_parallel_batches("index_finish_parallel_batches");
bvar::Adder<uint64_t> g_parallel_writers("index_finish_parallel_writers");
bvar::Adder<uint64_t> g_pool_rejected("index_finish_pool_rejected");

int wanted_threads() {
    return config::index_finish_thread_num > 0
                   ? config::index_finish_thread_num
                   : std::max(1, static_cast<int>(std::thread::hardware_concurrency()));
}

ThreadPool* finish_pool() {
    static std::once_flag once;
    static std::unique_ptr<ThreadPool> pool;
    std::call_once(once, [] {
        auto st = ThreadPoolBuilder("IndexFinishPool")
                          .set_min_threads(1)
                          .set_max_threads(wanted_threads())
                          .build(&pool);
        if (!st.ok()) {
            LOG(WARNING) << "failed to create the index finish pool: " << st;
            pool.reset();
        }
    });
    if (pool != nullptr && pool->max_threads() != wanted_threads()) {
        static std::mutex resize_mutex;
        std::lock_guard lock(resize_mutex);
        if (pool->max_threads() != wanted_threads()) {
            static_cast<void>(pool->set_max_threads(wanted_threads()));
        }
    }
    return pool.get();
}

// The writers of one run(), taken in turn by the threads that finish them.
struct FinishWork {
    explicit FinishWork(std::vector<IndexColumnWriter*> w) : writers(std::move(w)) {}
    std::vector<IndexColumnWriter*> writers;
    std::atomic<size_t> next {0};
    std::mutex status_mutex;
    Status status;

    void drain() {
        for (size_t i = next.fetch_add(1); i < writers.size(); i = next.fetch_add(1)) {
            IndexColumnWriter* writer = writers[i];
            Status st = run_catching_exceptions([writer]() { return writer->finish(); });
            if (!st.ok()) {
                std::lock_guard lock(status_mutex);
                if (status.ok()) {
                    status = std::move(st);
                }
            }
        }
    }
};

} // namespace

IndexFinishBatch::IndexFinishBatch() {
    if (config::index_finish_parallelism > 1) {
        _open = true;
        _previous = t_batch;
        t_batch = this;
    }
}

IndexFinishBatch::~IndexFinishBatch() {
    if (_open) {
        static_cast<void>(run());
    }
}

Status IndexFinishBatch::finish_index(IndexColumnWriter* writer) {
    if (t_batch != nullptr) {
        t_batch->_writers.push_back(writer);
        return Status::OK();
    }
    return writer->finish();
}

Status IndexFinishBatch::run() {
    if (!_open) {
        return Status::OK();
    }
    _open = false;
    t_batch = _previous;
    std::vector<IndexColumnWriter*> writers = std::move(_writers);
    _writers.clear();
    if (writers.empty()) {
        return Status::OK();
    }
    if (writers.size() > 1) {
        // The biggest writers, which take longest to finish, start first.
        std::stable_sort(writers.begin(), writers.end(),
                         [](const IndexColumnWriter* a, const IndexColumnWriter* b) {
                             return a->size() > b->size();
                         });
    }
    auto work = std::make_shared<FinishWork>(std::move(writers));
    const size_t helpers =
            std::min<size_t>(work->writers.size(),
                             static_cast<size_t>(std::max(1, config::index_finish_parallelism))) -
            1;
    ThreadPool* pool = helpers > 0 ? finish_pool() : nullptr;
    size_t submitted = 0;
    CountDownLatch done(static_cast<int>(helpers));
    if (pool != nullptr) {
        std::shared_ptr<MemTrackerLimiter> tracker;
        if (doris::pthread_context_ptr_init) {
            tracker = thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker_sptr();
        }
        for (size_t i = 0; i < helpers; ++i) {
            auto st = pool->submit_func([work, tracker, &done]() {
                if (tracker != nullptr) {
                    SCOPED_ATTACH_TASK(tracker);
                    work->drain();
                } else {
                    work->drain();
                }
                done.count_down();
            });
            if (!st.ok()) {
                g_pool_rejected << 1;
                break;
            }
            ++submitted;
        }
    }
    for (size_t i = submitted; i < helpers; ++i) {
        done.count_down();
    }
    g_parallel_batches << 1;
    g_parallel_writers << work->writers.size();
    work->drain();
    done.wait();
    return work->status;
}

} // namespace doris::segment_v2
