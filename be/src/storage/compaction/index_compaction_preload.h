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

#include <cstdint>
#include <string>
#include <vector>

#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"

namespace doris {

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
void preload_index_files(const std::vector<IndexFilePreloadTarget>& targets);

// preload_index_files() for files already open. Readers that do not read through the file cache
// are skipped.
void preload_cached_files(const std::vector<io::FileReaderSPtr>& readers);

} // namespace doris
