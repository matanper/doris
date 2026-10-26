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
#include <exception>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/block_compression.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace doris::segment_v2 {
namespace {

bvar::Adder<uint64_t> g_parallel_runs("segment_write_parallel_runs");
bvar::Adder<uint64_t> g_parallel_tasks("segment_write_parallel_tasks");
bvar::Adder<uint64_t> g_helper_tasks("segment_write_helper_tasks");
bvar::Adder<uint64_t> g_pool_rejected("segment_write_pool_rejected");
bvar::Adder<uint64_t> g_task_exceptions("segment_write_task_exceptions");
// Tasks of ChunkedSegmentWriteTasks run in chunks on the task group, and run on the writing
// thread because all of them together were less than two chunks.
bvar::Adder<uint64_t> g_chunk_tasks("segment_write_chunked_tasks");
bvar::Adder<uint64_t> g_inline_tasks("segment_write_inline_tasks");

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

} // namespace

Status run_catching_exceptions(const std::function<Status()>& task) {
    try {
        return task();
    } catch (const doris::Exception& e) {
        g_task_exceptions << 1;
        if (e.code() == doris::ErrorCode::MEM_ALLOC_FAILED) {
            return Status::MemoryLimitExceeded("segment write task: {}", e.to_string());
        }
        return Status::Error<false>(e.code(), e.to_string());
    } catch (const std::exception& e) {
        g_task_exceptions << 1;
        return Status::InternalError<false>("segment write task threw: {}", e.what());
    } catch (...) {
        g_task_exceptions << 1;
        return Status::InternalError<false>("segment write task threw an unknown exception");
    }
}

struct SegmentWriteTaskGroup::State {
    std::mutex mutex;
    std::condition_variable idle;
    std::condition_variable work;
    std::deque<std::function<Status()>> queue;
    int active = 0;
    bool closed = false;
    Status status;

    void record(Status st) {
        if (!st.ok()) {
            std::lock_guard lock(mutex);
            if (status.ok()) {
                status = std::move(st);
            }
        }
    }

    // Runs queued tasks until the queue is empty. The caller holds no lock.
    void drain() {
        for (;;) {
            std::function<Status()> task;
            {
                std::lock_guard lock(mutex);
                if (queue.empty()) {
                    return;
                }
                task = std::move(queue.front());
                queue.pop_front();
            }
            record(run_catching_exceptions(task));
        }
    }

    // Runs tasks as they are queued until the group is closed and its queue is empty.
    void help() {
        {
            std::lock_guard lock(mutex);
            if (closed) {
                return;
            }
            ++active;
        }
        for (;;) {
            std::function<Status()> task;
            {
                std::unique_lock lock(mutex);
                work.wait(lock, [&] { return !queue.empty() || closed; });
                if (queue.empty()) {
                    break;
                }
                task = std::move(queue.front());
                queue.pop_front();
            }
            record(run_catching_exceptions(task));
        }
        std::lock_guard lock(mutex);
        if (--active == 0) {
            idle.notify_all();
        }
    }
};

SegmentWriteTaskGroup::SegmentWriteTaskGroup(int parallelism)
        : _state(std::make_shared<State>()), _parallelism(std::max(1, parallelism)) {}

SegmentWriteTaskGroup::~SegmentWriteTaskGroup() {
    if (!_waited) {
        static_cast<void>(wait());
    }
}

void SegmentWriteTaskGroup::add(std::function<Status()> task) {
    {
        std::lock_guard lock(_state->mutex);
        _state->queue.push_back(std::move(task));
    }
    _state->work.notify_one();
    g_parallel_tasks << 1;
    if (_helpers + 1 >= _parallelism) {
        return;
    }
    ThreadPool* pool = write_pool();
    if (pool == nullptr) {
        return;
    }
    std::shared_ptr<MemTrackerLimiter> tracker;
    if (doris::pthread_context_ptr_init) {
        tracker = thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker_sptr();
    }
    const int zstd_level = current_zstd_compression_level();
    auto st = pool->submit_func([state = _state, tracker, zstd_level]() {
        ScopedZstdCompressionLevel level(zstd_level);
        if (tracker != nullptr) {
            SCOPED_ATTACH_TASK(tracker);
            state->help();
        } else {
            state->help();
        }
    });
    if (st.ok()) {
        ++_helpers;
        g_helper_tasks << 1;
    } else {
        g_pool_rejected << 1;
    }
}

Status SegmentWriteTaskGroup::wait() {
    _waited = true;
    g_parallel_runs << 1;
    _state->drain();
    {
        std::lock_guard lock(_state->mutex);
        _state->closed = true;
    }
    _state->work.notify_all();
    std::unique_lock lock(_state->mutex);
    _state->idle.wait(lock, [&] { return _state->active == 0; });
    return _state->status;
}

ChunkedSegmentWriteTasks::ChunkedSegmentWriteTasks(int parallelism)
        : _parallelism(parallelism),
          _min_bytes(static_cast<size_t>(std::max<int64_t>(0, config::segment_write_task_min_bytes))) {}

ChunkedSegmentWriteTasks::~ChunkedSegmentWriteTasks() {
    if (!_waited) {
        static_cast<void>(wait());
    }
}

Status ChunkedSegmentWriteTasks::run_chunk(const Chunk& chunk) {
    for (const auto& task : chunk) {
        RETURN_IF_ERROR(run_catching_exceptions(task));
    }
    return Status::OK();
}

void ChunkedSegmentWriteTasks::submit(Chunk chunk) {
    g_chunk_tasks << chunk.size();
    if (chunk.size() == 1) {
        _group->add(std::move(chunk.front()));
        return;
    }
    _group->add([chunk = std::move(chunk)]() { return run_chunk(chunk); });
}

void ChunkedSegmentWriteTasks::add(size_t bytes, std::function<Status()> task) {
    if (_parallelism <= 1) {
        if (_inline_status.ok()) {
            _inline_status = run_catching_exceptions(task);
        }
        return;
    }
    _current.push_back(std::move(task));
    _current_bytes += bytes;
    if (_current_bytes < _min_bytes) {
        return;
    }
    if (_group == nullptr && _held.empty()) {
        _held = std::move(_current);
    } else {
        if (_group == nullptr) {
            _group = std::make_unique<SegmentWriteTaskGroup>(_parallelism);
            submit(std::move(_held));
        }
        submit(std::move(_current));
    }
    _current = Chunk {};
    _current_bytes = 0;
}

Status ChunkedSegmentWriteTasks::wait() {
    _waited = true;
    if (_group == nullptr) {
        g_inline_tasks << _held.size() + _current.size();
        Status st = _inline_status;
        if (st.ok()) {
            st = run_chunk(_held);
        }
        if (st.ok()) {
            st = run_chunk(_current);
        }
        _held.clear();
        _current.clear();
        return st;
    }
    if (!_current.empty()) {
        submit(std::move(_current));
        _current = Chunk {};
    }
    return _group->wait();
}

Status run_segment_write_tasks(size_t n, int parallelism,
                               const std::function<Status(size_t)>& task) {
    if (n == 0) {
        return Status::OK();
    }
    SegmentWriteTaskGroup group(std::min<int>(parallelism, static_cast<int>(n)));
    for (size_t i = 0; i < n; ++i) {
        group.add([&task, i]() { return task(i); });
    }
    return group.wait();
}

} // namespace doris::segment_v2
