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
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>

#include "common/status.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/io_common.h"

namespace doris {
class MemTrackerLimiter;
} // namespace doris

namespace doris::io {

// A whole remote file held in memory for the compaction that reads it, so that its reads neither
// go to the file cache nor fill it. The file is loaded in chunks: a chunk is loaded once, by
// whichever comes first of the thread it was queued on and a read that needs it, and every load
// reads through `reader` with FileCacheMissPolicy::REMOTE_ONLY_ON_MISS, so a chunk the file cache
// holds whole is copied from it and any other chunk is read from remote storage without being
// written to the cache.
class MemoryPreloadedFile {
public:
    // The memory is charged to the memory tracker of the calling thread's task until the file is
    // destroyed. nullptr when it cannot be allocated.
    static std::shared_ptr<MemoryPreloadedFile> create(FileReaderSPtr reader, size_t chunk_size);

    MemoryPreloadedFile(const MemoryPreloadedFile&) = delete;
    MemoryPreloadedFile& operator=(const MemoryPreloadedFile&) = delete;
    ~MemoryPreloadedFile();

    size_t size() const { return _size; }
    size_t num_chunks() const { return _num_chunks; }

    // Loads chunk `index` unless a thread has already claimed it.
    void load_chunk(size_t index);
    // Marks chunk `index` failed unless a thread has already claimed it, so that its reads go
    // through the file cache instead.
    void skip_chunk(size_t index);

    // Copies [offset, offset + n) into `dst`, loading the chunks no thread has claimed and
    // waiting for the ones being loaded. False when the range is past the end of the file or a
    // chunk of it failed to load; `dst` is then unspecified.
    bool read(size_t offset, char* dst, size_t n);

private:
    enum ChunkState : uint8_t { PENDING, LOADING, READY, FAILED };

    MemoryPreloadedFile(FileReaderSPtr reader, size_t size, size_t chunk_size,
                        std::unique_ptr<char[]> data, std::shared_ptr<MemTrackerLimiter> tracker);
    // Loads chunk `index`, which the caller has moved from PENDING to LOADING.
    void _load_claimed_chunk(size_t index);
    void _finish_chunk(size_t index, ChunkState state);

    const FileReaderSPtr _reader;
    const size_t _size;
    const size_t _chunk_size;
    const size_t _num_chunks;
    const std::unique_ptr<char[]> _data;
    const std::unique_ptr<std::atomic<uint8_t>[]> _states;
    const std::shared_ptr<MemTrackerLimiter> _tracker;
    std::mutex _mutex;
    std::condition_variable _chunk_done;
};

// Whether reads of `reader_type` are served by the registered MemoryPreloadedFiles.
bool reads_memory_preloaded_files(ReaderType reader_type);

// The file a CachedRemoteFileReader with cache hash `key` reads from memory, while its compaction
// holds it registered. Only reads_memory_preloaded_files() reads look it up, and a file being
// compacted is read by no other compaction, so a registered file is read only by the compaction
// that registered it.
void register_memory_preloaded_file(const UInt128Wrapper& key,
                                    std::shared_ptr<MemoryPreloadedFile> file);
void unregister_memory_preloaded_file(const UInt128Wrapper& key);
// nullptr when no file is registered under `key`.
std::shared_ptr<MemoryPreloadedFile> find_memory_preloaded_file(const UInt128Wrapper& key);

} // namespace doris::io
