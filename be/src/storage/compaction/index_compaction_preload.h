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

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"

namespace doris {

// The file cache queue a preload fills, which must be the one the later reads use.
enum class PreloadCacheQueue {
    INDEX,      // inverted index files, read with is_index_data
    DISPOSABLE, // segment data files read by compaction, read with is_disposable
};

// One file for preload_index_files().
struct IndexFilePreloadTarget {
    io::FileSystemSPtr fs;
    std::string path;
    int64_t file_size = -1; // -1 when unknown
    int64_t tablet_id = 0;
};

// Reads each file once through the file cache, so that later small reads of it hit local cache
// blocks. A file is read in chunks of config::index_compaction_preload_chunk_bytes as dry runs:
// the missing blocks of a chunk are downloaded into the cache, one remote read per run of
// adjacent missing blocks, and nothing is copied out, so a file already cached costs only block
// lookups. The chunks run on a pool of config::index_compaction_preload_threads threads shared by
// all callers, which bounds the concurrent remote reads and their buffers. Returns when every
// chunk is done; a failed chunk leaves its blocks to the reads that need them. Files that are not
// read through the file cache are skipped.
void preload_index_files(const std::vector<IndexFilePreloadTarget>& targets,
                         PreloadCacheQueue queue = PreloadCacheQueue::INDEX);

// preload_index_files() for files already open. Readers that do not read through the file cache
// are skipped.
void preload_cached_files(const std::vector<io::FileReaderSPtr>& readers,
                          PreloadCacheQueue queue = PreloadCacheQueue::INDEX);

// A preload its caller does not wait for. Destroying it, or cancel(), skips the chunks no thread
// has started; a chunk being read finishes on its own.
class PreloadTask {
public:
    struct State;
    explicit PreloadTask(std::shared_ptr<State> state) : _state(std::move(state)) {}
    ~PreloadTask() { cancel(); }
    PreloadTask(const PreloadTask&) = delete;
    PreloadTask& operator=(const PreloadTask&) = delete;

    void cancel();
    // Returns when every chunk was read or skipped.
    void wait();

private:
    std::shared_ptr<State> _state;
};

// preload_cached_files() without waiting. The chunks of all files are queued by their relative
// position in their file, so the start of every file comes first, as the first column groups of a
// vertical compaction need. Returns nullptr when there is nothing to preload.
std::unique_ptr<PreloadTask> start_preload_cached_files(
        const std::vector<io::FileReaderSPtr>& readers,
        PreloadCacheQueue queue = PreloadCacheQueue::INDEX);

// preload_index_files() without waiting.
std::unique_ptr<PreloadTask> start_preload_index_files(
        const std::vector<IndexFilePreloadTarget>& targets,
        PreloadCacheQueue queue = PreloadCacheQueue::INDEX);

// The order start_preload_cached_files() queues the chunks of files of `file_sizes` bytes in, as
// (file index, offset) pairs.
std::vector<std::pair<size_t, size_t>> preload_chunk_order(const std::vector<size_t>& file_sizes,
                                                           size_t chunk_size);

int preload_pool_max_threads_for_test();

} // namespace doris
