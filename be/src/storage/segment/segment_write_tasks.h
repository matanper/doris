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

#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "common/status.h"

namespace doris::segment_v2 {

// Tasks of one segment write, run on the shared SegmentWritePool and on the calling thread, at
// most `parallelism` at once. Tasks may be added while earlier ones run, and wait() returns when
// all have run, with the first error. A task that throws fails with the exception's status
// instead of ending the process. Pool threads account their memory on the calling thread's
// memory tracker and compress at its ZSTD level. A pool thread that has not started by the time
// the calling thread runs out of tasks does nothing, so callers on pool threads cannot deadlock
// waiting for each other. The tasks of a group must touch disjoint state.
class SegmentWriteTaskGroup {
public:
    explicit SegmentWriteTaskGroup(int parallelism);
    // Waits for the tasks still running.
    ~SegmentWriteTaskGroup();
    SegmentWriteTaskGroup(const SegmentWriteTaskGroup&) = delete;
    SegmentWriteTaskGroup& operator=(const SegmentWriteTaskGroup&) = delete;

    void add(std::function<Status()> task);
    Status wait();

    struct State;

private:
    std::shared_ptr<State> _state;
    int _parallelism;
    int _helpers = 0;
    bool _waited = false;
};

// Tasks of one segment write, each with the bytes of input it handles, grouped in the order added
// into chunks of at least config::segment_write_task_min_bytes. The tasks of a chunk run one after
// another on one thread, and the chunks run as the tasks of a SegmentWriteTaskGroup. While the
// tasks added fill at most one chunk they run on the calling thread, in wait(), and no pool thread
// is used; with parallelism <= 1 each task runs in add(). After a task fails, the tasks of its
// chunk after it do not run. The tasks must touch disjoint state.
class ChunkedSegmentWriteTasks {
public:
    explicit ChunkedSegmentWriteTasks(int parallelism);
    ~ChunkedSegmentWriteTasks();
    ChunkedSegmentWriteTasks(const ChunkedSegmentWriteTasks&) = delete;
    ChunkedSegmentWriteTasks& operator=(const ChunkedSegmentWriteTasks&) = delete;

    void add(size_t bytes, std::function<Status()> task);
    Status wait();

private:
    using Chunk = std::vector<std::function<Status()>>;
    static Status run_chunk(const Chunk& chunk);
    void submit(Chunk chunk);

    const int _parallelism;
    const size_t _min_bytes;
    Chunk _current;
    size_t _current_bytes = 0;
    // The first full chunk, kept back until a second one fills.
    Chunk _held;
    std::unique_ptr<SegmentWriteTaskGroup> _group;
    Status _inline_status;
    bool _waited = false;
};

// Runs task(i) for every i in [0, n) as one SegmentWriteTaskGroup.
Status run_segment_write_tasks(size_t n, int parallelism, const std::function<Status(size_t)>& task);

// Runs `task`, turning an exception it throws into a failed status.
Status run_catching_exceptions(const std::function<Status()>& task);

} // namespace doris::segment_v2
