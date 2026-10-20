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

// Runs task(i) for every i in [0, n) as one SegmentWriteTaskGroup.
Status run_segment_write_tasks(size_t n, int parallelism, const std::function<Status(size_t)>& task);

// Runs `task`, turning an exception it throws into a failed status.
Status run_catching_exceptions(const std::function<Status()>& task);

} // namespace doris::segment_v2
