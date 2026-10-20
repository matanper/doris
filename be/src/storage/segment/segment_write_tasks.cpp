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

#include "storage/segment/segment_write_tasks.h"

#include <bvar/bvar.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

#include "common/config.h"
#include "common/logging.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/block_compression.h"
#include "util/threadpool.h"

namespace doris::segment_v2 {
namespace {

bvar::Adder<uint64_t> g_parallel_runs("segment_write_parallel_runs");
bvar::Adder<uint64_t> g_parallel_tasks("segment_write_parallel_tasks");
bvar::Adder<uint64_t> g_helper_tasks("segment_write_helper_tasks");
bvar::Adder<uint64_t> g_pool_rejected("segment_write_pool_rejected");

int wanted_threads() {
    return config::segment_write_thread_num > 0
                   ? config::segment_write_thread_num
                   : std::max(1, static_cast<int>(std::thread::hardware_concurrency()));
}

ThreadPool* write_pool() {
    static std::once_flag once;
    static std::unique_ptr<ThreadPool> pool;
    std::call_once(once, [] {
        auto st = ThreadPoolBuilder("SegmentWritePool")
                          .set_min_threads(1)
                          .set_max_threads(wanted_threads())
                          .build(&pool);
        if (!st.ok()) {
            LOG(WARNING) << "failed to create the segment write pool: " << st;
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

struct TaskRun {
    TaskRun(size_t n_, const std::function<Status(size_t)>* task_) : n(n_), task(task_) {}
    const size_t n;
    const std::function<Status(size_t)>* const task;
    std::atomic<size_t> next {0};
    std::mutex mutex;
    std::condition_variable idle;
    int active = 0;
    bool closed = false;
    Status status;

    void drain() {
        for (size_t i = next.fetch_add(1); i < n; i = next.fetch_add(1)) {
            Status st = (*task)(i);
            if (!st.ok()) {
                std::lock_guard lock(mutex);
                if (status.ok()) {
                    status = std::move(st);
                }
            }
        }
    }

    // A helper works only if the caller has not finished yet.
    void help() {
        {
            std::lock_guard lock(mutex);
            if (closed) {
                return;
            }
            ++active;
        }
        drain();
        std::lock_guard lock(mutex);
        if (--active == 0) {
            idle.notify_all();
        }
    }
};

} // namespace

Status run_segment_write_tasks(size_t n, int parallelism,
                               const std::function<Status(size_t)>& task) {
    if (n == 0) {
        return Status::OK();
    }
    const size_t helpers = std::min<size_t>(n, static_cast<size_t>(std::max(1, parallelism))) - 1;
    ThreadPool* pool = helpers > 0 ? write_pool() : nullptr;
    if (pool == nullptr) {
        for (size_t i = 0; i < n; ++i) {
            RETURN_IF_ERROR(task(i));
        }
        return Status::OK();
    }
    auto run = std::make_shared<TaskRun>(n, &task);
    std::shared_ptr<MemTrackerLimiter> tracker;
    if (doris::pthread_context_ptr_init) {
        tracker = thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker_sptr();
    }
    // Pages compress at the calling thread's ZSTD level.
    const int zstd_level = current_zstd_compression_level();
    for (size_t i = 0; i < helpers; ++i) {
        auto st = pool->submit_func([run, tracker, zstd_level]() {
            ScopedZstdCompressionLevel level(zstd_level);
            if (tracker != nullptr) {
                SCOPED_ATTACH_TASK(tracker);
                run->help();
            } else {
                run->help();
            }
        });
        if (!st.ok()) {
            g_pool_rejected << 1;
            break;
        }
        g_helper_tasks << 1;
    }
    g_parallel_runs << 1;
    g_parallel_tasks << n;
    run->drain();
    std::unique_lock lock(run->mutex);
    run->closed = true;
    run->idle.wait(lock, [&] { return run->active == 0; });
    return run->status;
}

} // namespace doris::segment_v2
