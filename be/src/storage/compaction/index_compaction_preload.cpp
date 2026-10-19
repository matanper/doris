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

#include "storage/compaction/index_compaction_preload.h"

#include <bvar/bvar.h>

#include <algorithm>
#include <memory>
#include <mutex>

#include "common/config.h"
#include "common/logging.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/file_reader.h"
#include "io/io_common.h"
#include "util/countdown_latch.h"
#include "util/slice.h"
#include "util/stopwatch.hpp"
#include "util/threadpool.h"

namespace doris {
namespace {

bvar::Adder<uint64_t> g_preload_failed_chunks("index_compaction_preload_failed_chunks");
bvar::LatencyRecorder g_preload_latency_us("index_compaction_preload_latency_us");
bvar::Adder<uint64_t> g_data_preload_files("compaction_data_preload_files");
bvar::Adder<uint64_t> g_data_preload_bytes("compaction_data_preload_bytes");
// Bytes of the preloaded files that were already in the file cache, by the queue holding them.
// Load and compaction outputs are written to the normal or TTL queue, while blocks filled by
// index reads go to the index queue.
bvar::Adder<uint64_t> g_preload_cached_normal_bytes("index_compaction_preload_cached_normal_bytes");
bvar::Adder<uint64_t> g_preload_cached_ttl_bytes("index_compaction_preload_cached_ttl_bytes");
bvar::Adder<uint64_t> g_preload_cached_index_bytes("index_compaction_preload_cached_index_bytes");
bvar::Adder<uint64_t> g_preload_cached_disposable_bytes(
        "index_compaction_preload_cached_disposable_bytes");

void count_cached_bytes(const io::FileReader& reader) {
    auto hash = io::BlockFileCache::hash(io::remote_file_cache_name(reader.path().native()));
    auto* cache = io::FileCacheFactory::instance()->get_by_path(hash);
    if (cache == nullptr) {
        return;
    }
    for (const auto& [_, block] : cache->get_blocks_by_key(hash)) {
        if (block->state() != io::FileBlock::State::DOWNLOADED) {
            continue;
        }
        const size_t bytes = block->range().size();
        switch (block->cache_type()) {
        case io::FileCacheType::NORMAL:
            g_preload_cached_normal_bytes << bytes;
            break;
        case io::FileCacheType::TTL:
            g_preload_cached_ttl_bytes << bytes;
            break;
        case io::FileCacheType::INDEX:
            g_preload_cached_index_bytes << bytes;
            break;
        case io::FileCacheType::DISPOSABLE:
            g_preload_cached_disposable_bytes << bytes;
            break;
        }
    }
}

ThreadPool* preload_pool() {
    static std::once_flag once;
    static std::unique_ptr<ThreadPool> pool;
    std::call_once(once, [] {
        auto st = ThreadPoolBuilder("IndexCompactionPreload")
                          .set_min_threads(1)
                          .set_max_threads(std::max(1, config::index_compaction_preload_threads))
                          .build(&pool);
        if (!st.ok()) {
            LOG(WARNING) << "failed to create the index compaction preload pool: " << st;
            pool.reset();
        }
    });
    return pool.get();
}

struct PreloadChunk {
    io::FileReaderSPtr reader;
    size_t offset;
    size_t size;
};

} // namespace

void preload_cached_files(const std::vector<io::FileReaderSPtr>& readers, PreloadCacheQueue queue) {
    ThreadPool* pool = config::enable_file_cache ? preload_pool() : nullptr;
    if (pool == nullptr || readers.empty()) {
        return;
    }
    MonotonicStopWatch watch;
    watch.start();
    const auto chunk_size = static_cast<size_t>(
            std::max<int64_t>(config::index_compaction_preload_chunk_bytes, 1 << 20));
    std::vector<PreloadChunk> chunks;
    for (const auto& reader : readers) {
        if (dynamic_cast<io::CachedRemoteFileReader*>(reader.get()) == nullptr) {
            continue;
        }
        if (queue == PreloadCacheQueue::DISPOSABLE) {
            g_data_preload_files << 1;
            g_data_preload_bytes << reader->size();
        }
        count_cached_bytes(*reader);
        for (size_t offset = 0; offset < reader->size(); offset += chunk_size) {
            chunks.push_back({reader, offset, std::min(chunk_size, reader->size() - offset)});
        }
    }
    CountDownLatch latch(static_cast<int>(chunks.size()));
    for (const auto& chunk : chunks) {
        auto st = pool->submit_func([chunk, &latch, queue]() {
            io::IOContext io_ctx;
            io_ctx.is_dryrun = true;
            // The later reads use the same flags, so missed blocks land in the same cache queue
            // either way.
            io_ctx.is_index_data = queue == PreloadCacheQueue::INDEX;
            io_ctx.is_disposable = queue == PreloadCacheQueue::DISPOSABLE;
            // A cache block is ready once the dry run returns.
            io_ctx.cache_write_mode_override = io::CacheWriteMode::SYNC_WRITE;
            size_t bytes_read = 0;
            if (!chunk.reader
                         ->read_at(chunk.offset, Slice((char*)nullptr, chunk.size), &bytes_read,
                                   &io_ctx)
                         .ok()) {
                g_preload_failed_chunks << 1;
            }
            latch.count_down();
        });
        if (!st.ok()) {
            g_preload_failed_chunks << 1;
            latch.count_down();
        }
    }
    latch.wait();
    g_preload_latency_us << watch.elapsed_time() / 1000;
}

void preload_index_files(const std::vector<IndexFilePreloadTarget>& targets,
                         PreloadCacheQueue queue) {
    if (!config::enable_file_cache || targets.empty()) {
        return;
    }
    std::vector<io::FileReaderSPtr> readers;
    for (const auto& target : targets) {
        io::FileReaderOptions opts;
        opts.cache_type = io::FileCachePolicy::FILE_BLOCK_CACHE;
        opts.is_doris_table = true;
        opts.file_size = target.file_size;
        opts.tablet_id = target.tablet_id;
        io::FileReaderSPtr reader;
        if (target.fs != nullptr && target.fs->open_file(target.path, &reader, &opts).ok()) {
            readers.push_back(std::move(reader));
        }
    }
    preload_cached_files(readers, queue);
}

} // namespace doris
