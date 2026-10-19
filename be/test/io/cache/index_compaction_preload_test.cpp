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
#include <memory>
#include <string>

#include "block_file_cache_test_common.h"
#include "cloud/config.h"
#include "common/config.h"
#include "io/cache/cached_remote_file_reader.h"
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

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override {
        ++_reads;
        _bytes += result.size;
        return _delegate->read_at(offset, result, bytes_read, io_ctx);
    }

private:
    FileReaderSPtr _delegate;
    std::atomic<size_t> _reads {0};
    std::atomic<size_t> _bytes {0};
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

} // namespace doris::io
