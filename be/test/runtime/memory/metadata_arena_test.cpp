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

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet/tablet_schema.h"

#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER) && \
        defined(USE_JEMALLOC)
#include "jemalloc/jemalloc.h"
#define METADATA_ARENA_TEST_JEMALLOC 1
#endif

namespace doris {

namespace {

// The arena that owns ptr, or MetadataArena::kNoArena when it cannot be told.
unsigned owning_arena(const void* ptr) {
#ifdef METADATA_ARENA_TEST_JEMALLOC
    unsigned index = 0;
    size_t size = sizeof(index);
    if (jemallctl("arenas.lookup", &index, &size, &ptr, sizeof(ptr)) == 0) {
        return index;
    }
#endif
    (void)ptr;
    return MetadataArena::kNoArena;
}

} // namespace

TEST(MetadataArenaTest, AllocatesWritableMemoryAndFreesIt) {
    std::vector<void*> ptrs;
    for (size_t size : {1, 24, 384, 896, 4096, 70000}) {
        void* ptr = MetadataArena::allocate(size);
        ASSERT_NE(ptr, nullptr);
        std::memset(ptr, 0xab, size);
        EXPECT_EQ(owning_arena(ptr), MetadataArena::arena_index());
        ptrs.push_back(ptr);
    }
    for (void* ptr : ptrs) {
        MetadataArena::deallocate(ptr);
    }
    MetadataArena::deallocate(nullptr);
}

TEST(MetadataArenaTest, RowsetMetaComesFromTheArenaHoweverItIsCreated) {
    RowsetMetaSharedPtr shared = RowsetMeta::create_shared();
    EXPECT_EQ(owning_arena(shared.get()), MetadataArena::arena_index());

    RowsetMetaSharedPtr from_new(new RowsetMeta());
    EXPECT_EQ(owning_arena(from_new.get()), MetadataArena::arena_index());

    shared->set_num_rows(7);
    EXPECT_EQ(shared->num_rows(), 7);
}

TEST(MetadataArenaTest, RowsetFromTheFactoryComesFromTheArena) {
    auto schema = std::make_shared<TabletSchema>();
    RowsetMetaSharedPtr meta = RowsetMeta::create_shared();
    meta->set_rowset_type(BETA_ROWSET);
    RowsetSharedPtr rowset;
    ASSERT_TRUE(RowsetFactory::create_rowset(schema, "", meta, &rowset).ok());
    EXPECT_EQ(owning_arena(rowset.get()), MetadataArena::arena_index());
    // shared_from_this still works for a rowset whose control block came from the arena.
    EXPECT_EQ(rowset->shared_from_this().get(), rowset.get());
}

TEST(MetadataArenaTest, ObjectsCanBeFreedOnAnotherThread) {
    std::vector<RowsetMetaSharedPtr> metas;
    for (int i = 0; i < 1000; ++i) {
        metas.push_back(RowsetMeta::create_shared());
    }
    std::thread([&metas] { metas.clear(); }).join();
    EXPECT_TRUE(metas.empty());

    // The regions freed by the other thread are reused by later allocations of the same size.
    for (int i = 0; i < 1000; ++i) {
        metas.push_back(RowsetMeta::create_shared());
    }
    metas.clear();
}

} // namespace doris
