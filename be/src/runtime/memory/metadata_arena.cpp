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

#include "runtime/memory/metadata_arena.h"

#include <bvar/passive_status.h>
#include <glog/logging.h>

#include <new>
#include <string>

#include "common/config.h"

#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER) && \
        defined(USE_JEMALLOC)
#include "jemalloc/jemalloc.h"
#define DORIS_METADATA_ARENA_JEMALLOC 1
#endif

namespace doris {

namespace {

unsigned create_arena() {
#ifdef DORIS_METADATA_ARENA_JEMALLOC
    if (!config::enable_rowset_metadata_arena) {
        return MetadataArena::kNoArena;
    }
    unsigned index = 0;
    size_t size = sizeof(index);
    if (jemallctl("arenas.create", &index, &size, nullptr, 0) != 0) {
        LOG(WARNING) << "Failed to create the rowset metadata jemalloc arena, using the global heap";
        return MetadataArena::kNoArena;
    }
    LOG(INFO) << "Rowset metadata uses jemalloc arena " << index;
    return index;
#else
    return MetadataArena::kNoArena;
#endif
}

unsigned arena() {
    static const unsigned index = create_arena();
    return index;
}

#ifdef DORIS_METADATA_ARENA_JEMALLOC
int64_t arena_stat(const char* name) {
    unsigned index = arena();
    if (index == MetadataArena::kNoArena) {
        return 0;
    }
    size_t value = 0;
    size_t size = sizeof(value);
    std::string key = "stats.arenas." + std::to_string(index) + "." + name;
    if (jemallctl(key.c_str(), &value, &size, nullptr, 0) != 0) {
        return 0;
    }
    return static_cast<int64_t>(value);
}

size_t mallctl_size(const std::string& name) {
    size_t value = 0;
    size_t size = sizeof(value);
    return jemallctl(name.c_str(), &value, &size, nullptr, 0) == 0 ? value : 0;
}

// Bytes of the slabs the arena holds for small objects, live or not. Against
// rowset_metadata_arena_small_allocated_bytes it gives how full those slabs are.
int64_t arena_slab_bytes() {
    unsigned index = arena();
    if (index == MetadataArena::kNoArena) {
        return 0;
    }
    unsigned nbins = 0;
    size_t size = sizeof(nbins);
    if (jemallctl("arenas.nbins", &nbins, &size, nullptr, 0) != 0) {
        return 0;
    }
    const std::string prefix = "stats.arenas." + std::to_string(index) + ".bins.";
    int64_t total = 0;
    for (unsigned bin = 0; bin < nbins; ++bin) {
        size_t curslabs = mallctl_size(prefix + std::to_string(bin) + ".curslabs");
        if (curslabs != 0) {
            total += static_cast<int64_t>(
                    curslabs * mallctl_size("arenas.bin." + std::to_string(bin) + ".slab_size"));
        }
    }
    return total;
}

// Refreshed with the rest of jemalloc's statistics, whenever the BE advances the stats epoch.
bvar::PassiveStatus<int64_t> g_metadata_arena_small_allocated(
        "rowset_metadata_arena_small_allocated_bytes",
        [](void*) { return arena_stat("small.allocated"); }, nullptr);
bvar::PassiveStatus<int64_t> g_metadata_arena_large_allocated(
        "rowset_metadata_arena_large_allocated_bytes",
        [](void*) { return arena_stat("large.allocated"); }, nullptr);
bvar::PassiveStatus<int64_t> g_metadata_arena_slab_bytes("rowset_metadata_arena_slab_bytes",
                                                         [](void*) { return arena_slab_bytes(); },
                                                         nullptr);
#endif

} // namespace

void* MetadataArena::allocate(size_t size) {
#ifdef DORIS_METADATA_ARENA_JEMALLOC
    unsigned index = arena();
    if (index != kNoArena) {
        void* ptr = jemallocx(size == 0 ? 1 : size, MALLOCX_ARENA(index) | MALLOCX_TCACHE_NONE);
        if (ptr == nullptr) [[unlikely]] {
            throw std::bad_alloc();
        }
        return ptr;
    }
#endif
    return ::operator new(size);
}

void* MetadataArena::allocate_nothrow(size_t size) noexcept {
#ifdef DORIS_METADATA_ARENA_JEMALLOC
    unsigned index = arena();
    if (index != kNoArena) {
        return jemallocx(size == 0 ? 1 : size, MALLOCX_ARENA(index) | MALLOCX_TCACHE_NONE);
    }
#endif
    return ::operator new(size, std::nothrow);
}

void MetadataArena::deallocate(void* ptr) noexcept {
    if (ptr == nullptr) {
        return;
    }
#ifdef DORIS_METADATA_ARENA_JEMALLOC
    if (arena() != kNoArena) {
        jedallocx(ptr, MALLOCX_TCACHE_NONE);
        return;
    }
#endif
    ::operator delete(ptr);
}

unsigned MetadataArena::arena_index() {
    return arena();
}

} // namespace doris
