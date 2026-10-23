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

#include <gen_cpp/cloud.pb.h>

#include <cstdint>
#include <string>
#include <string_view>

#include "common/status.h"
#include "io/fs/file_system.h"

namespace doris::io {

constexpr uint32_t kPackedFileTrailerVersion = 1;
constexpr size_t kPackedFileTrailerSuffixSize = sizeof(uint32_t) * 2;

Status parse_packed_file_trailer(std::string_view data, cloud::PackedFileFooterPB* debug_pb,
                                 uint32_t* version);

Status read_packed_file_trailer(const std::string& file_path, cloud::PackedFileFooterPB* debug_pb,
                                uint32_t* version);

// Reads the trailer of a packed file through fs, bypassing the file cache. packed_file_size may
// be -1 when unknown.
Status read_packed_file_trailer(const FileSystemSPtr& fs, const std::string& packed_file_path,
                                int64_t packed_file_size, cloud::PackedFileFooterPB* footer,
                                uint32_t* version);

} // namespace doris::io
