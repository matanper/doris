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
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "block_file_cache_test_common.h"
#include "cloud/config.h"
#include "common/config.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/cache/memory_preloaded_file.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "io/fs/local_file_system.h"

namespace doris::io {
namespace {

// Counts the reads that reach the wrapped "remote" file.
class CountingRemoteReader final : public FileReader {
public:
    explicit CountingRemoteReader(FileReaderSPtr delegate) : _delegate(std::move(delegate)) {}

    Status close() override { return _delegate->close(); }
    const Path& path() const override { return _delegate->path(); }
    size_t size() const override { return _delegate->size(); }
    bool closed() const override { return _delegate->closed(); }
    int64_t mtime() const override { return _delegate->mtime(); }

    size_t reads() const { return _reads; }
    size_t bytes() const { return _bytes; }
    // Reads at or past `offset` fail.
    void fail_from(size_t offset) { _fail_from = offset; }
    // Each read first sleeps this long.
    void set_delay_ms(int ms) { _delay_ms = ms; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override {
        if (_delay_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(_delay_ms.load()));
        }
        if (offset >= _fail_from) {
            return Status::IOError("injected read failure at {}", offset);
        }
        ++_reads;
        _bytes += result.size;
        return _delegate->read_at(offset, result, bytes_read, io_ctx);
    }

private:
    FileReaderSPtr _delegate;
    std::atomic<size_t> _reads {0};
    std::atomic<size_t> _bytes {0};
    std::atomic<size_t> _fail_from {SIZE_MAX};
    std::atomic<int> _delay_ms {0};
};

class IndexCompactionPreloadTest : public BlockFileCacheTest {
protected:
    void SetUp() override {
        _old_async = config::enable_async_file_cache_write;
        _old_direct = config::enable_read_cache_file_directly;
        _old_peer = config::enable_cache_read_from_peer;
        _old_block_size = config::file_cache_each_block_size;
        _old_chunk = config::index_compaction_preload_chunk_bytes;
        _old_enable_cache = config::enable_file_cache;
        _old_threads = config::index_compaction_preload_threads;
        config::enable_async_file_cache_write = false;
        config::enable_read_cache_file_directly = false;
        config::enable_cache_read_from_peer = false;
        config::file_cache_each_block_size = 1_mb;
        config::index_compaction_preload_chunk_bytes = 4_mb;
        config::enable_file_cache = true;
        reset_factory();
        ExecEnv::GetInstance()->set_file_cache_open_fd_cache(std::make_unique<io::FDCache>());

        _cache_path = caches_dir / "index_compaction_preload";
        std::error_code error;
        fs::remove_all(_cache_path, error);
        fs::create_directories(_cache_path);
        FileCacheSettings settings;
        settings.query_queue_size = 16_mb;
        settings.query_queue_elements = 32;
        settings.index_queue_size = 16_mb;
        settings.index_queue_elements = 32;
        settings.disposable_queue_size = 4_mb;
        settings.disposable_queue_elements = 8;
        settings.capacity = 40_mb;
        settings.max_file_block_size = 1_mb;
        settings.max_query_cache_size = 0;
        ASSERT_TRUE(FileCacheFactory::instance()
                            ->create_file_cache(_cache_path.string(), settings)
                            .ok());
        auto* cache = FileCacheFactory::instance()->_path_to_cache[_cache_path.string()];
        ASSERT_NE(cache, nullptr);
        wait_until_cache_ready(*cache);
    }

    void TearDown() override {
        reset_factory();
        std::error_code error;
        fs::remove_all(_cache_path, error);
        config::enable_async_file_cache_write = _old_async;
        config::enable_read_cache_file_directly = _old_direct;
        config::enable_cache_read_from_peer = _old_peer;
        config::file_cache_each_block_size = _old_block_size;
        config::index_compaction_preload_chunk_bytes = _old_chunk;
        config::enable_file_cache = _old_enable_cache;
        config::index_compaction_preload_threads = _old_threads;
    }

    static void reset_factory() {
        FileCacheFactory::instance()->_caches.clear();
        FileCacheFactory::instance()->_path_to_cache.clear();
        FileCacheFactory::instance()->_capacity = 0;
    }

    std::shared_ptr<CountingRemoteReader> open_remote() {
        FileReaderSPtr local;
        EXPECT_TRUE(global_local_filesystem()->open_file(tmp_file, &local).ok());
        return std::make_shared<CountingRemoteReader>(std::move(local));
    }

    static int64_t exposed(const std::string& name) {
        return std::stoll(bvar::Variable::describe_exposed(name));
    }

    static FileReaderSPtr cached(FileReaderSPtr remote) {
        FileReaderOptions options;
        options.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
        options.is_doris_table = true;
        options.tablet_id = 10087;
        return std::make_shared<CachedRemoteFileReader>(std::move(remote), options);
    }

private:
    fs::path _cache_path;
    bool _old_async = false;
    bool _old_direct = false;
    bool _old_peer = false;
    int64_t _old_block_size = 0;
    int64_t _old_chunk = 0;
    bool _old_enable_cache = false;
    int32_t _old_threads = 64;
};

} // namespace

// The whole file is fetched in a few chunk-sized remote reads, after which the small reads an
// index merge makes are all served by the cache, and preloading again fetches nothing.
TEST_F(IndexCompactionPreloadTest, preload_fetches_file_in_chunks_then_reads_hit_cache) {
    auto remote = open_remote();
    auto reader = cached(remote);
    const size_t file_size = reader->size();
    // The fixture is 10 MB and one byte.
    ASSERT_GT(file_size, 10_mb);
    ASSERT_LE(file_size, 12_mb);

    preload_cached_files({reader});
    EXPECT_EQ(remote->reads(), 3); // 4 MB + 4 MB + the rest
    EXPECT_EQ(remote->bytes(), file_size);

    IOContext io_ctx;
    io_ctx.is_index_data = true;
    std::string buffer(4096, '\0');
    for (size_t offset = 0; offset < file_size; offset += 300 * 1024) {
        size_t bytes_read = 0;
        ASSERT_TRUE(
                reader->read_at(offset, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx)
                        .ok());
        EXPECT_EQ(buffer[0], static_cast<char>('0' + offset / 1_mb));
    }
    EXPECT_EQ(remote->reads(), 3);

    const int64_t cached_before = exposed("index_compaction_preload_cached_index_bytes");
    preload_cached_files({reader});
    EXPECT_EQ(remote->reads(), 3);
    // The second preload found the whole file in the index queue.
    EXPECT_EQ(exposed("index_compaction_preload_cached_index_bytes") - cached_before,
              static_cast<int64_t>(file_size));
}

// Only the blocks not cached yet are fetched.
TEST_F(IndexCompactionPreloadTest, preload_fetches_only_missing_blocks) {
    auto remote = open_remote();
    auto reader = cached(remote);
    IOContext io_ctx;
    io_ctx.is_index_data = true;
    std::string buffer(1_mb, '\0');
    size_t bytes_read = 0;
    ASSERT_TRUE(reader->read_at(0, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());
    ASSERT_EQ(remote->reads(), 1);

    preload_cached_files({reader});
    // 1-4 MB in the first chunk, then the next two chunks.
    EXPECT_EQ(remote->reads(), 4);
    EXPECT_EQ(remote->bytes(), reader->size());
}

// Data files fill the disposable queue, which compaction reads use.
TEST_F(IndexCompactionPreloadTest, data_preload_fills_disposable_queue) {
    auto remote = open_remote();
    auto reader = cached(remote);
    preload_cached_files({reader}, PreloadCacheQueue::DISPOSABLE);
    EXPECT_EQ(remote->reads(), 3);

    IOContext io_ctx;
    io_ctx.is_disposable = true;
    std::string buffer(64 * 1024, '\0');
    for (size_t offset = 0; offset < reader->size(); offset += 1_mb) {
        size_t bytes_read = 0;
        ASSERT_TRUE(
                reader->read_at(offset, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx)
                        .ok());
    }
    EXPECT_EQ(remote->reads(), 3);

    const int64_t cached_before = exposed("index_compaction_preload_cached_disposable_bytes");
    preload_cached_files({reader}, PreloadCacheQueue::DISPOSABLE);
    EXPECT_EQ(exposed("index_compaction_preload_cached_disposable_bytes") - cached_before,
              static_cast<int64_t>(reader->size()));
}

// Readers that do not go through the file cache, and a disabled file cache, are skipped.
TEST_F(IndexCompactionPreloadTest, preload_skips_uncached_readers) {
    auto remote = open_remote();
    preload_cached_files({remote});
    EXPECT_EQ(remote->reads(), 0);

    config::enable_file_cache = false;
    auto reader = cached(remote);
    preload_cached_files({reader});
    EXPECT_EQ(remote->reads(), 0);
}

// The chunks of all files are queued by their position relative to their file's size.
TEST_F(IndexCompactionPreloadTest, chunks_are_interleaved_by_relative_offset) {
    const auto order = preload_chunk_order({10_mb, 5_mb}, 4_mb);
    const std::vector<std::pair<size_t, size_t>> expected = {
            {0, 0}, {1, 0}, {0, 4_mb}, {0, 8_mb}, {1, 4_mb}};
    EXPECT_EQ(order, expected);
    EXPECT_TRUE(preload_chunk_order({0, 0}, 4_mb).empty());
}

// Reads made while the preload is still downloading return the file's bytes, and every block is
// fetched once.
TEST_F(IndexCompactionPreloadTest, reads_during_async_preload_see_the_file) {
    auto remote = open_remote();
    remote->set_delay_ms(20);
    auto reader = cached(remote);
    const size_t file_size = reader->size();
    auto task = start_preload_cached_files({reader}, PreloadCacheQueue::DISPOSABLE);
    ASSERT_NE(task, nullptr);

    IOContext io_ctx;
    io_ctx.is_disposable = true;
    std::string buffer(4096, '\0');
    for (size_t offset = 0; offset < file_size; offset += 300 * 1024) {
        size_t bytes_read = 0;
        ASSERT_TRUE(
                reader->read_at(offset, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx)
                        .ok());
        EXPECT_EQ(buffer[0], static_cast<char>('0' + offset / 1_mb));
    }
    task->wait();
    EXPECT_EQ(remote->bytes(), file_size);
}

// A cancelled preload skips the chunks no thread has started.
TEST_F(IndexCompactionPreloadTest, cancelled_preload_skips_unstarted_chunks) {
    config::index_compaction_preload_threads = 1;
    ASSERT_EQ(preload_pool_max_threads_for_test(), 1);
    // Idle threads above the pool's one minimum thread leave after its idle timeout.
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    auto remote = open_remote();
    remote->set_delay_ms(200);
    auto reader = cached(remote);
    const int64_t skipped_before = exposed("index_compaction_preload_skipped_chunks");
    {
        auto task = start_preload_cached_files({reader}, PreloadCacheQueue::DISPOSABLE);
        ASSERT_NE(task, nullptr);
        task->cancel();
        task->wait();
    }
    EXPECT_GE(exposed("index_compaction_preload_skipped_chunks") - skipped_before, 2);
    EXPECT_LT(remote->bytes(), reader->size());
    // Dropping a task cancels it as well.
    auto other = cached(open_remote());
    {
        auto dropped = start_preload_cached_files({other}, PreloadCacheQueue::DISPOSABLE);
    }
    config::index_compaction_preload_threads = 64;
}

// A chunk that fails leaves its blocks to later reads; the rest of the preload still lands.
TEST_F(IndexCompactionPreloadTest, failed_chunk_leaves_the_rest_cached) {
    auto remote = open_remote();
    remote->fail_from(8_mb);
    auto reader = cached(remote);
    const int64_t failed_before = exposed("index_compaction_preload_failed_chunks");
    auto task = start_preload_cached_files({reader}, PreloadCacheQueue::DISPOSABLE);
    ASSERT_NE(task, nullptr);
    task->wait();
    EXPECT_EQ(exposed("index_compaction_preload_failed_chunks") - failed_before, 1);
    const size_t fetched = remote->reads();
    IOContext io_ctx;
    io_ctx.is_disposable = true;
    std::string buffer(64 * 1024, '\0');
    for (size_t offset = 0; offset < 8_mb; offset += 1_mb) {
        size_t bytes_read = 0;
        ASSERT_TRUE(
                reader->read_at(offset, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx)
                        .ok());
    }
    EXPECT_EQ(remote->reads(), fetched) << "the first 8 MB were cached";
}

// The pool follows index_compaction_preload_threads at runtime.
TEST_F(IndexCompactionPreloadTest, pool_size_follows_config) {
    config::index_compaction_preload_threads = 3;
    auto reader = cached(open_remote());
    preload_cached_files({reader}, PreloadCacheQueue::DISPOSABLE);
    EXPECT_EQ(preload_pool_max_threads_for_test(), 3);
    config::index_compaction_preload_threads = 64;
    preload_cached_files({cached(open_remote())}, PreloadCacheQueue::DISPOSABLE);
    EXPECT_EQ(preload_pool_max_threads_for_test(), 64);

    // 0 sizes it from the compaction slots, with at least 64 threads.
    const int32_t old_slots = config::compaction_task_num_per_fast_disk;
    config::index_compaction_preload_threads = 0;
    config::compaction_task_num_per_fast_disk = 80;
    EXPECT_EQ(preload_pool_max_threads_for_test(), 160);
    config::compaction_task_num_per_fast_disk = 8;
    EXPECT_EQ(preload_pool_max_threads_for_test(), 64);
    config::compaction_task_num_per_fast_disk = old_slots;
}

namespace {

std::vector<FileBlockSPtr> cached_blocks(const FileReader& reader) {
    auto hash = BlockFileCache::hash(remote_file_cache_name(reader.path().native()));
    std::vector<FileBlockSPtr> blocks;
    for (auto& [_, block] : FileCacheFactory::instance()->get_by_path(hash)->get_blocks_by_key(hash)) {
        blocks.push_back(block);
    }
    return blocks;
}

Status compaction_read(const FileReaderSPtr& reader, size_t offset, std::string* buffer) {
    IOContext io_ctx;
    io_ctx.reader_type = ReaderType::READER_CUMULATIVE_COMPACTION;
    io_ctx.is_disposable = true;
    size_t bytes_read = 0;
    return reader->read_at(offset, Slice(buffer->data(), buffer->size()), &bytes_read, &io_ctx);
}

class MemoryPreloadConfig {
public:
    MemoryPreloadConfig(int64_t task_bytes, int64_t total_bytes)
            : _task(config::compaction_memory_preload_task_bytes),
              _total(config::compaction_memory_preload_total_bytes) {
        config::compaction_memory_preload_task_bytes = task_bytes;
        config::compaction_memory_preload_total_bytes = total_bytes;
    }
    ~MemoryPreloadConfig() {
        config::compaction_memory_preload_task_bytes = _task;
        config::compaction_memory_preload_total_bytes = _total;
    }

private:
    int64_t _task;
    int64_t _total;
};

} // namespace

// A compaction's reads of a file loaded into memory are served from there: the file is fetched
// once, in chunks, nothing is written to the file cache, and the memory is charged to the task's
// tracker until the preload ends, after which the reads go through the file cache again.
TEST_F(IndexCompactionPreloadTest, memory_preload_serves_compaction_reads_without_cache_writes) {
    MemoryPreloadConfig budget(1LL << 30, 1LL << 30);
    auto remote = open_remote();
    auto reader = cached(remote);
    const size_t file_size = reader->size();
    auto tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::COMPACTION, "MemoryPreload");
    SCOPED_ATTACH_TASK(tracker);
    const int64_t before = tracker->consumption();

    std::vector<size_t> not_taken;
    auto task = start_memory_preload_files({reader}, &not_taken);
    ASSERT_NE(task, nullptr);
    EXPECT_TRUE(not_taken.empty());
    EXPECT_GE(tracker->consumption() - before, static_cast<int64_t>(file_size));

    const int64_t served_before = exposed("cached_remote_reader_memory_preloaded_read_bytes");
    std::string buffer(4096, '\0');
    size_t bytes = 0;
    for (size_t offset = 0; offset < file_size; offset += 300 * 1024) {
        ASSERT_TRUE(compaction_read(reader, offset, &buffer).ok());
        EXPECT_EQ(buffer[0], static_cast<char>('0' + offset / 1_mb));
        bytes += std::min(buffer.size(), file_size - offset);
    }
    EXPECT_EQ(exposed("cached_remote_reader_memory_preloaded_read_bytes") - served_before,
              static_cast<int64_t>(bytes));
    EXPECT_EQ(remote->reads(), 3); // 4 MB + 4 MB + the rest, by the pool or by the reads
    EXPECT_EQ(remote->bytes(), file_size);
    EXPECT_TRUE(cached_blocks(*reader).empty());

    task.reset();
    EXPECT_LT(tracker->consumption() - before, static_cast<int64_t>(file_size));
    EXPECT_EQ(io::find_memory_preloaded_file(
                      BlockFileCache::hash(remote_file_cache_name(reader->path().native()))),
              nullptr);
    ASSERT_TRUE(compaction_read(reader, 0, &buffer).ok());
    EXPECT_EQ(remote->reads(), 4);
    EXPECT_FALSE(cached_blocks(*reader).empty());
}

// The chunks the file cache holds whole are copied from it, not fetched again.
TEST_F(IndexCompactionPreloadTest, memory_preload_reads_cached_chunks_from_the_cache) {
    MemoryPreloadConfig budget(1LL << 30, 1LL << 30);
    auto remote = open_remote();
    auto reader = cached(remote);
    preload_cached_files({reader});
    ASSERT_EQ(remote->reads(), 3);

    std::vector<size_t> not_taken;
    auto task = start_memory_preload_files({reader}, &not_taken);
    ASSERT_NE(task, nullptr);
    std::string buffer(4096, '\0');
    for (size_t offset = 0; offset < 10_mb; offset += 1_mb) {
        ASSERT_TRUE(compaction_read(reader, offset, &buffer).ok());
        EXPECT_EQ(buffer[0], static_cast<char>('0' + offset / 1_mb));
    }
    EXPECT_EQ(remote->reads(), 3);
}

// A file that does not fit in the task's or the process's budget is not taken, and the budget a
// preload holds is free again once it ends.
TEST_F(IndexCompactionPreloadTest, memory_preload_keeps_within_its_budgets) {
    auto remote = open_remote();
    auto reader = cached(remote);
    const auto file_size = static_cast<int64_t>(reader->size());
    std::vector<size_t> not_taken;
    {
        MemoryPreloadConfig budget(file_size - 1, 1LL << 30);
        EXPECT_EQ(start_memory_preload_files({reader}, &not_taken), nullptr);
        EXPECT_EQ(not_taken, std::vector<size_t> {0});
    }
    MemoryPreloadConfig budget(1LL << 30, file_size + file_size / 2);
    auto first = start_memory_preload_files({reader}, &not_taken);
    ASSERT_NE(first, nullptr);
    EXPECT_TRUE(not_taken.empty());
    EXPECT_EQ(start_memory_preload_files({cached(open_remote())}, &not_taken), nullptr);
    EXPECT_EQ(not_taken, std::vector<size_t> {0});
    first.reset();
    EXPECT_NE(start_memory_preload_files({cached(open_remote())}, &not_taken), nullptr);
}

// The merge loads the chunks it reaches before the pool does, and a chunk that fails to load
// leaves its reads to the file cache.
TEST_F(IndexCompactionPreloadTest, memory_preload_reads_load_unstarted_chunks_and_fall_back) {
    MemoryPreloadConfig budget(1LL << 30, 1LL << 30);
    config::index_compaction_preload_threads = 1;
    auto remote = open_remote();
    remote->set_delay_ms(300);
    remote->fail_from(8_mb);
    auto reader = cached(remote);
    const int64_t self_loaded = exposed("memory_preloaded_file_loaded_by_reader_chunks");
    const int64_t fallbacks = exposed("cached_remote_reader_memory_preloaded_fallback_reads");
    std::vector<size_t> not_taken;
    auto task = start_memory_preload_files({reader}, &not_taken);
    ASSERT_NE(task, nullptr);
    std::string buffer(4096, '\0');
    // The single pool thread starts on the first chunk; the read of the second one loads it.
    ASSERT_TRUE(compaction_read(reader, 5_mb, &buffer).ok());
    EXPECT_EQ(buffer[0], '5');
    EXPECT_GE(exposed("memory_preloaded_file_loaded_by_reader_chunks") - self_loaded, 1);
    EXPECT_FALSE(compaction_read(reader, 9_mb, &buffer).ok());
    EXPECT_EQ(exposed("cached_remote_reader_memory_preloaded_fallback_reads") - fallbacks, 1);
    task.reset();
    config::index_compaction_preload_threads = 64;
}

} // namespace doris::io
