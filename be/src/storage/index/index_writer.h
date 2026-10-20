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

#include <butil/macros.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <thread>
#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "storage/olap_common.h"
#include "storage/options.h"

namespace doris {
#include "common/compile_check_begin.h"

class TabletIndex;
class TabletColumn;

namespace segment_v2 {
class IndexFileWriter;

class IndexColumnWriter {
public:
    static Status create(const TabletColumn* column, std::unique_ptr<IndexColumnWriter>* res,
                         IndexFileWriter* index_file_writer, const TabletIndex* inverted_index);
    virtual Status init() = 0;

    IndexColumnWriter() = default;
    virtual ~IndexColumnWriter() = default;

    virtual Status add_values(const std::string name, const void* values, size_t count) = 0;

    virtual Status add_array_values(size_t field_size, const void* value_ptr,
                                    const uint8_t* null_map, const uint8_t* offsets_ptr,
                                    size_t count) = 0;

    virtual Status add_nulls(uint32_t count) = 0;
    virtual Status add_array_nulls(const uint8_t* null_map, size_t num_rows) = 0;

    virtual Status finish() = 0;

    virtual int64_t size() const = 0;

    virtual void close_on_error() = 0;

    // check if the column is valid for inverted index, some columns
    // are generated from variant, but not all of them are supported
    static bool check_support_inverted_index(const TabletColumn& column);

    static bool check_support_ann_index(const TabletColumn& column);
};

// Names of top-level entries of the index tmp dirs that live writers use, as prefixes: an index
// file writer's "<rowset>_<segment>_" and an index compaction's "<rowset>". The orphan sweeper
// never removes an entry a registered prefix covers.
class IndexTmpRegistry {
public:
    static IndexTmpRegistry& instance();

    void add(const std::string& prefix);
    void remove(const std::string& prefix);
    // Whether a registered prefix is a prefix of `name`.
    bool covers(const std::string& name) const;

private:
    mutable std::mutex _mutex;
    std::map<std::string, int> _prefixes;
};

// Registers a prefix in IndexTmpRegistry for its lifetime.
class ScopedIndexTmpPrefix {
public:
    explicit ScopedIndexTmpPrefix(std::string prefix) : _prefix(std::move(prefix)) {
        IndexTmpRegistry::instance().add(_prefix);
    }
    ~ScopedIndexTmpPrefix() { IndexTmpRegistry::instance().remove(_prefix); }
    ScopedIndexTmpPrefix(const ScopedIndexTmpPrefix&) = delete;
    ScopedIndexTmpPrefix& operator=(const ScopedIndexTmpPrefix&) = delete;

private:
    std::string _prefix;
};

// The local directories index writers and index compaction put their temporary files in:
// <storage_root_path>/<tmp_file_dir> for every store path, or the dirs of
// config::index_tmp_file_dirs when it is set. Each is emptied at startup.
class TmpFileDirs {
public:
    TmpFileDirs(const std::vector<doris::StorePath>& store_paths);
    // Stops the orphan sweeper.
    ~TmpFileDirs();

    // Empties and creates the dirs. With config::index_tmp_file_dirs, refuses, before deleting
    // anything, a dir validate_index_tmp_dir() rejects, and fails when a dir cannot be written.
    Status init();

    // The next dir, round-robin. With config::index_tmp_file_dirs, a dir whose file system has
    // less than config::index_tmp_file_dir_min_free_bytes free is passed over; when all are,
    // a store path's tmp dir is returned.
    io::Path get_tmp_file_dir();

    // Why `dir` may not be an index tmp dir: it must be absolute, must not equal or contain a
    // store path, must not equal, contain or lie inside a file cache path or a protected dir
    // (log, conf) other than being exactly <file cache path>/index_tmp, and must not be a mount
    // point.
    static Status validate_index_tmp_dir(const std::string& dir,
                                         const std::vector<std::string>& store_paths,
                                         const std::vector<std::string>& cache_paths,
                                         const std::vector<std::string>& protected_dirs);

    // Replaces the free-space probe of the index tmp dirs.
    void set_free_bytes_function_for_test(std::function<int64_t(const io::Path&)> fn) {
        _free_bytes_fn = std::move(fn);
    }

    const std::vector<io::Path>& index_tmp_dirs() const { return _index_tmp_dirs; }

    // Starts a thread that runs sweep_orphans() every index_tmp_orphan_sweep_interval_seconds.
    void start_orphan_sweeper();

    // Removes the top-level entries of every tmp dir that no registered prefix covers and that
    // nothing under has changed for index_tmp_orphan_max_age_minutes: what failed or cancelled
    // writers left. Symbolic links are left alone and never followed. Returns the entries
    // removed.
    size_t sweep_orphans();

private:
    int64_t free_bytes(size_t index);

    std::vector<io::Path> _tmp_file_dirs;
    std::atomic_size_t _next_index {0}; // use for round-robin
    std::vector<io::Path> _index_tmp_dirs;
    std::atomic_size_t _next_index_tmp {0};
    std::function<int64_t(const io::Path&)> _free_bytes_fn;
    // Free bytes of each index tmp dir's file system, and when they were read, in ms.
    std::unique_ptr<std::atomic<int64_t>[]> _free;
    std::unique_ptr<std::atomic<int64_t>[]> _free_read_ms;
    std::mutex _sweeper_mutex;
    std::condition_variable _sweeper_cv;
    bool _sweeper_stop = false;
    std::thread _sweeper;
};

} // namespace segment_v2

#include "common/compile_check_end.h"
} // namespace doris
