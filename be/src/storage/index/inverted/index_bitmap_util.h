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

#include <cstddef>
#include <optional>
#include <roaring/roaring.hh>
#include <string>
#include <string_view>

namespace doris::segment_v2 {

// A bitmap serialized by Roaring::write(buf, false), read from at most `size` bytes. None when
// the bytes are not a valid such bitmap. Roaring::read(buf, false) instead reads as many bytes as
// the data claims, so corrupt bytes make it read past the buffer or throw std::runtime_error.
inline std::optional<roaring::Roaring> read_native_roaring(const char* buf, size_t size) {
    roaring::api::roaring_bitmap_t* bitmap =
            roaring::api::roaring_bitmap_deserialize_safe(buf, size);
    if (bitmap == nullptr) {
        return std::nullopt;
    }
    return roaring::Roaring(bitmap);
}

// The first `max_bytes` bytes in hex, for corruption reports.
inline std::string hex_prefix(std::string_view bytes, size_t max_bytes = 32) {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string out;
    const size_t n = bytes.size() < max_bytes ? bytes.size() : max_bytes;
    out.reserve(n * 2);
    for (size_t i = 0; i < n; ++i) {
        const auto c = static_cast<unsigned char>(bytes[i]);
        out.push_back(kDigits[c >> 4]);
        out.push_back(kDigits[c & 0xf]);
    }
    return out;
}

} // namespace doris::segment_v2
