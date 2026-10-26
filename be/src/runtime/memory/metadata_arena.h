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
#include <cstdint>
#include <limits>
#include <new>

namespace doris {

// Allocates long-lived rowset metadata (RowsetMeta, Rowset) from a jemalloc arena of its own.
//
// A BE keeps millions of these objects alive for as long as their rowsets exist, while loads,
// compactions and scans allocate and free short-lived objects of the same size classes all the
// time. Sharing slabs with that churn leaves each slab holding a few surviving rowset objects
// after the short-lived neighbours are freed, and a slab is only returned when all of its
// regions are free. Keeping the long-lived objects in their own arena lets the churn reuse and
// return its slabs.
//
// Allocations bypass the thread cache, so a freed region goes straight back to this arena
// whichever thread frees it, and is never handed out by another thread's cache for an
// unrelated object. Builds without jemalloc, and BEs started with
// enable_rowset_metadata_arena=false, use the global operator new and delete instead. The
// choice is made once, on first use, so every pointer is freed the way it was allocated.
class MetadataArena {
public:
    static void* allocate(size_t size);
    // nullptr instead of std::bad_alloc when out of memory.
    static void* allocate_nothrow(size_t size) noexcept;
    static void deallocate(void* ptr) noexcept;

    // The jemalloc arena index, or kNoArena when the global heap is used.
    static unsigned arena_index();

    static constexpr unsigned kNoArena = std::numeric_limits<unsigned>::max();
};

// Class-scope operator new and delete that put every object of the class, and of classes
// derived from it, in MetadataArena. Placement new keeps working.
#define DORIS_METADATA_ARENA_OPERATORS                                                     \
    static void* operator new(size_t size) { return ::doris::MetadataArena::allocate(size); } \
    static void* operator new(size_t size, const std::nothrow_t&) noexcept {                  \
        return ::doris::MetadataArena::allocate_nothrow(size);                                \
    }                                                                                         \
    static void* operator new(size_t, void* ptr) noexcept { return ptr; }                     \
    static void operator delete(void* ptr) noexcept {                                         \
        ::doris::MetadataArena::deallocate(ptr);                                              \
    }                                                                                         \
    static void operator delete(void* ptr, const std::nothrow_t&) noexcept {                  \
        ::doris::MetadataArena::deallocate(ptr);                                              \
    }                                                                                         \
    static void operator delete(void*, void*) noexcept {}

// Standard allocator over MetadataArena, for std::allocate_shared and for the control block
// of a std::shared_ptr built from a raw pointer.
template <typename T>
struct MetadataArenaAllocator {
    using value_type = T;

    MetadataArenaAllocator() = default;
    template <typename U>
    MetadataArenaAllocator(const MetadataArenaAllocator<U>&) noexcept {}

    T* allocate(size_t n) {
        static_assert(alignof(T) <= alignof(std::max_align_t));
        return static_cast<T*>(MetadataArena::allocate(n * sizeof(T)));
    }
    void deallocate(T* ptr, size_t) noexcept { MetadataArena::deallocate(ptr); }

    template <typename U>
    bool operator==(const MetadataArenaAllocator<U>&) const noexcept {
        return true;
    }
    template <typename U>
    bool operator!=(const MetadataArenaAllocator<U>&) const noexcept {
        return false;
    }
};

} // namespace doris
