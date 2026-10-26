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

#include "io/cache/memory_preloaded_file.h"

#include <bvar/bvar.h>

#include <algorithm>
#include <cstring>
#include <shared_mutex>
#include <unordered_map>

#include "common/logging.h"
#include "io/fs/file_reader.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/slice.h"
#include "util/stopwatch.hpp"

namespace doris::io {
namespace {

bvar::Adder<int64_t> g_resident_bytes("memory_preloaded_file_resident_bytes");
bvar::Adder<uint64_t> g_loaded_bytes("memory_preloaded_file_loaded_bytes");
bvar::Adder<uint64_t> g_loaded_by_reader_chunks("memory_preloaded_file_loaded_by_reader_chunks");
bvar::Adder<uint64_t> g_failed_chunks("memory_preloaded_file_failed_chunks");
bvar::Adder<uint64_t> g_skipped_chunks("memory_preloaded_file_skipped_chunks");
bvar::Adder<uint64_t> g_alloc_failures("memory_preloaded_file_alloc_failures");
bvar::LatencyRecorder g_wait_us("memory_preloaded_file_wait_us");

struct Registry {
    std::shared_mutex mutex;
    std::unordered_map<UInt128Wrapper, std::shared_ptr<MemoryPreloadedFile>, KeyHash> files;
};

Registry& registry() {
    static Registry instance;
    return instance;
}

} // namespace

std::shared_ptr<MemoryPreloadedFile> MemoryPreloadedFile::create(FileReaderSPtr reader,
                                                                 size_t chunk_size) {
    const size_t size = reader->size();
    std::unique_ptr<char[]> data;
    try {
        // Left uninitialized: every byte is written by its chunk's load before any read of it.
        data.reset(new char[std::max<size_t>(size, 1)]);
    } catch (...) {
        g_alloc_failures << 1;
        return nullptr;
    }
    std::shared_ptr<MemTrackerLimiter> tracker;
    if (doris::pthread_context_ptr_init) {
        tracker = doris::thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker_sptr();
    }
    return std::shared_ptr<MemoryPreloadedFile>(
            new MemoryPreloadedFile(std::move(reader), size, std::max<size_t>(chunk_size, 1),
                                    std::move(data), std::move(tracker)));
}

MemoryPreloadedFile::MemoryPreloadedFile(FileReaderSPtr reader, size_t size, size_t chunk_size,
                                         std::unique_ptr<char[]> data,
                                         std::shared_ptr<MemTrackerLimiter> tracker)
        : _reader(std::move(reader)),
          _size(size),
          _chunk_size(chunk_size),
          _num_chunks((size + chunk_size - 1) / chunk_size),
          _data(std::move(data)),
          _states(new std::atomic<uint8_t>[std::max<size_t>(_num_chunks, 1)]),
          _tracker(std::move(tracker)) {
    if (_tracker != nullptr) {
        _tracker->consume(static_cast<int64_t>(_size));
    }
    for (size_t i = 0; i < _num_chunks; ++i) {
        _states[i].store(PENDING, std::memory_order_relaxed);
    }
    g_resident_bytes << static_cast<int64_t>(_size);
}

MemoryPreloadedFile::~MemoryPreloadedFile() {
    if (_tracker != nullptr) {
        _tracker->release(static_cast<int64_t>(_size));
    }
    g_resident_bytes << -static_cast<int64_t>(_size);
}

void MemoryPreloadedFile::load_chunk(size_t index) {
    uint8_t expected = PENDING;
    if (_states[index].compare_exchange_strong(expected, LOADING)) {
        _load_claimed_chunk(index);
    }
}

void MemoryPreloadedFile::skip_chunk(size_t index) {
    uint8_t expected = PENDING;
    if (_states[index].compare_exchange_strong(expected, LOADING)) {
        g_skipped_chunks << 1;
        _finish_chunk(index, FAILED);
    }
}

void MemoryPreloadedFile::_load_claimed_chunk(size_t index) {
    const size_t offset = index * _chunk_size;
    const size_t length = std::min(_chunk_size, _size - offset);
    IOContext io_ctx;
    // Not a compaction reader type, so this read is not served from this file.
    io_ctx.reader_type = ReaderType::UNKNOWN;
    io_ctx.file_cache_miss_policy = FileCacheMissPolicy::REMOTE_ONLY_ON_MISS;
    size_t bytes_read = 0;
    Status st = _reader->read_at(offset, Slice(_data.get() + offset, length), &bytes_read, &io_ctx);
    if (st.ok() && bytes_read != length) {
        st = Status::IOError("short read of {}: {} of {} bytes at {}", _reader->path().native(),
                             bytes_read, length, offset);
    }
    if (!st.ok()) {
        g_failed_chunks << 1;
        LOG_EVERY_N(WARNING, 100) << "failed to load " << _reader->path().native() << " at "
                                  << offset << " into memory, its reads use the file cache: "
                                  << st;
        _finish_chunk(index, FAILED);
        return;
    }
    g_loaded_bytes << length;
    _finish_chunk(index, READY);
}

void MemoryPreloadedFile::_finish_chunk(size_t index, ChunkState state) {
    {
        std::lock_guard lock(_mutex);
        _states[index].store(state, std::memory_order_release);
    }
    _chunk_done.notify_all();
}

bool MemoryPreloadedFile::read(size_t offset, char* dst, size_t n) {
    if (offset > _size || n > _size - offset) {
        return false;
    }
    if (n == 0) {
        return true;
    }
    const size_t first = offset / _chunk_size;
    const size_t last = (offset + n - 1) / _chunk_size;
    for (size_t i = first; i <= last; ++i) {
        uint8_t state = _states[i].load(std::memory_order_acquire);
        if (state == PENDING) {
            uint8_t expected = PENDING;
            if (_states[i].compare_exchange_strong(expected, LOADING)) {
                g_loaded_by_reader_chunks << 1;
                _load_claimed_chunk(i);
            }
            state = _states[i].load(std::memory_order_acquire);
        }
        if (state == LOADING) {
            MonotonicStopWatch watch;
            watch.start();
            std::unique_lock lock(_mutex);
            _chunk_done.wait(lock, [&] {
                state = _states[i].load(std::memory_order_acquire);
                return state != LOADING;
            });
            g_wait_us << watch.elapsed_time() / 1000;
        }
        if (state != READY) {
            return false;
        }
    }
    memcpy(dst, _data.get() + offset, n);
    return true;
}

bool reads_memory_preloaded_files(ReaderType reader_type) {
    switch (reader_type) {
    case ReaderType::READER_BASE_COMPACTION:
    case ReaderType::READER_CUMULATIVE_COMPACTION:
    case ReaderType::READER_FULL_COMPACTION:
    case ReaderType::READER_COLD_DATA_COMPACTION:
        return true;
    default:
        return false;
    }
}

void register_memory_preloaded_file(const UInt128Wrapper& key,
                                    std::shared_ptr<MemoryPreloadedFile> file) {
    auto& r = registry();
    std::unique_lock lock(r.mutex);
    r.files.insert_or_assign(key, std::move(file));
}

void unregister_memory_preloaded_file(const UInt128Wrapper& key) {
    auto& r = registry();
    // Released after the lock, on the caller's thread.
    std::shared_ptr<MemoryPreloadedFile> removed;
    std::unique_lock lock(r.mutex);
    auto it = r.files.find(key);
    if (it == r.files.end()) {
        return;
    }
    removed = std::move(it->second);
    r.files.erase(it);
    lock.unlock();
}

std::shared_ptr<MemoryPreloadedFile> find_memory_preloaded_file(const UInt128Wrapper& key) {
    auto& r = registry();
    std::shared_lock lock(r.mutex);
    if (r.files.empty()) {
        return nullptr;
    }
    auto it = r.files.find(key);
    return it == r.files.end() ? nullptr : it->second;
}

} // namespace doris::io
