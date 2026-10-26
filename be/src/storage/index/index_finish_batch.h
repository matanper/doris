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

#include <vector>

#include "common/status.h"

namespace doris::segment_v2 {

class IndexColumnWriter;

// Which pool a batch's helpers run on. Loads and compactions have separate pools, so a memtable
// flush never waits behind compaction work queued in a shared pool.
enum class IndexFinishLane { LOAD, COMPACTION };

// Runs the finish() of a segment's index writers in parallel. While a batch is open on a thread,
// finish_index() on that thread queues the writer instead of finishing it, and run() then finishes
// the queued writers on the shared index finish pool and the calling thread, at most
// config::index_finish_parallelism at once, and returns when all are done. Each writer finishes
// into its own directory, and the compound index file is assembled afterwards in index order, so
// the output does not depend on the order the writers finish in. With index_finish_parallelism
// at most 1 no batch opens and finish_index() finishes at once. Loads and compactions finish on
// separate pools, and run() never waits for a pool thread that had not started by the time the
// calling thread ran out of writers.
class IndexFinishBatch {
public:
    explicit IndexFinishBatch(IndexFinishLane lane = IndexFinishLane::LOAD);
    // Finishes, on the calling thread, whatever run() was not called for.
    ~IndexFinishBatch();
    IndexFinishBatch(const IndexFinishBatch&) = delete;
    IndexFinishBatch& operator=(const IndexFinishBatch&) = delete;

    // Finishes the queued writers and closes the batch. Returns the first error.
    Status run();

    // Queues `writer` in the batch open on this thread, or finishes it if none is.
    static Status finish_index(IndexColumnWriter* writer);

private:
    IndexFinishLane _lane;
    bool _open = false;
    IndexFinishBatch* _previous = nullptr;
    std::vector<IndexColumnWriter*> _writers;
};

} // namespace doris::segment_v2
