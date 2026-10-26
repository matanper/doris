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

#include "storage/segment/segment_loader.h"

#include <butil/time.h>
#include <bvar/bvar.h>

#include <algorithm>

#include "common/config.h"
#include "common/status.h"
#include "storage/olap_define.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/segment/column_reader.h"
#include "util/stopwatch.hpp"

namespace doris {

namespace {
// Segment cache lookups and segment opens, by the kind of reader asking. A lookup that does not
// open the segment was a hit.
struct ReaderKindCounters {
    explicit ReaderKindCounters(const std::string& kind)
            : lookups("segment_cache_lookups_by_" + kind), loads("segment_loads_by_" + kind) {}
    bvar::Adder<int64_t> lookups;
    bvar::Adder<int64_t> loads;
};
ReaderKindCounters g_query_counters("query");
ReaderKindCounters g_compaction_counters("compaction");
ReaderKindCounters g_other_counters("other");
bvar::Adder<int64_t> g_segment_cache_recharges("segment_cache_recharges");
// The charges of the segments in the segment cache.
bvar::Adder<int64_t> g_segment_cache_charged_bytes("segment_cache_charged_bytes");

ReaderKindCounters& counters_for(const io::IOContext* io_ctx) {
    if (io_ctx == nullptr) {
        return g_other_counters;
    }
    if (io_ctx->reader_type == ReaderType::READER_QUERY) {
        return g_query_counters;
    }
    return segment_v2::ColumnReader::is_compaction_reader_type(io_ctx->reader_type)
                   ? g_compaction_counters
                   : g_other_counters;
}
} // namespace

SegmentLoader* SegmentLoader::instance() {
    return ExecEnv::GetInstance()->segment_loader();
}

bool SegmentCache::lookup(const SegmentCache::CacheKey& key, SegmentCacheHandle* handle) {
    auto* lru_handle = LRUCachePolicy::lookup(key.encode());
    if (lru_handle == nullptr) {
        return false;
    }
    handle->push_segment(this, lru_handle);
    return true;
}

SegmentCache::CacheValue::CacheValue(segment_v2::SegmentSharedPtr segment_)
        : segment(std::move(segment_)) {}

SegmentCache::CacheValue::~CacheValue() {
    g_segment_cache_charged_bytes << -charge;
}

void SegmentCache::insert(const SegmentCache::CacheKey& key, SegmentCache::CacheValue& value,
                          SegmentCacheHandle* handle) {
    // Charged with what the segment really holds, which the cache bound is then enforced on.
    value.charge = std::max<int64_t>(value.segment->real_mem_usage(), 1);
    value.segment->set_cache_charge(value.charge);
    g_segment_cache_charged_bytes << value.charge;
    auto* lru_handle = LRUCachePolicy::insert(key.encode(), &value, value.charge, value.charge,
                                              CachePriority::NORMAL);
    handle->push_segment(this, lru_handle);
}

bool SegmentCache::needs_recharge(int64_t real_bytes, int64_t charge) {
    return real_bytes > charge * config::segment_cache_recharge_ratio +
                                static_cast<double>(config::segment_cache_recharge_min_bytes);
}

void SegmentCache::erase(const SegmentCache::CacheKey& key) {
    LRUCachePolicy::erase(key.encode());
}

Status SegmentLoader::load_segment(const BetaRowsetSharedPtr& rowset, int64_t segment_id,
                                   SegmentCacheHandle* cache_handle, bool use_cache,
                                   bool need_load_pk_index_and_bf,
                                   OlapReaderStatistics* index_load_stats,
                                   const io::IOContext* io_ctx) {
    SegmentCache::CacheKey cache_key(rowset->rowset_id(), segment_id);
    auto& counters = counters_for(io_ctx);
    counters.lookups << 1;
    if (_segment_cache->lookup(cache_key, cache_handle)) {
        // Has to check the segment status here, because the segment in cache may has something wrong during
        // load index or create column reader.
        // Not merge this if logic with previous to make the logic more clear.
        if (cache_handle->pop_unhealthy_segment() == nullptr) {
            // A segment keeps growing after it is cached, as readers load its column readers and
            // external meta; re-insert it with its real size once that outgrows its charge, so
            // that the cache's memory bound holds.
            const auto& cached = cache_handle->get_segments().back();
            if (SegmentCache::needs_recharge(cached->real_mem_usage(), cached->cache_charge())) {
                g_segment_cache_recharges << 1;
                SegmentCacheHandle recharge_handle;
                _segment_cache->insert(cache_key, *new SegmentCache::CacheValue(cached),
                                       &recharge_handle);
            }
            return Status::OK();
        }
    }
    // If the segment is not healthy, then will create a new segment and will replace the unhealthy one in SegmentCache.
    segment_v2::SegmentSharedPtr segment;
    counters.loads << 1;
    RETURN_IF_ERROR(rowset->load_segment(segment_id, index_load_stats, &segment, io_ctx));
    if (need_load_pk_index_and_bf) {
        RETURN_IF_ERROR(segment->load_pk_index_and_bf(index_load_stats, io_ctx));
    }
    if (use_cache && !config::disable_segment_cache) {
        // memory of SegmentCache::CacheValue will be handled by SegmentCache
        segment->set_cache_loader(io_ctx != nullptr ? io_ctx->reader_type : ReaderType::UNKNOWN);
        auto* cache_value = new SegmentCache::CacheValue(segment);
        _cache_mem_usage += segment->meta_mem_usage();
        _segment_cache->insert(cache_key, *cache_value, cache_handle);
    } else {
        cache_handle->push_segment(std::move(segment));
    }

    return Status::OK();
}

Status SegmentLoader::load_segments(const BetaRowsetSharedPtr& rowset,
                                    SegmentCacheHandle* cache_handle, bool use_cache,
                                    bool need_load_pk_index_and_bf,
                                    OlapReaderStatistics* index_load_stats,
                                    const io::IOContext* io_ctx) {
    if (cache_handle->is_inited()) {
        return Status::OK();
    }
    for (int64_t i = 0; i < rowset->num_segments(); i++) {
        RETURN_IF_ERROR(load_segment(rowset, i, cache_handle, use_cache, need_load_pk_index_and_bf,
                                     index_load_stats, io_ctx));
    }
    cache_handle->set_inited();
    return Status::OK();
}

void SegmentLoader::erase_segment(const SegmentCache::CacheKey& key) {
    _segment_cache->erase(key);
}

void SegmentLoader::erase_segments(const RowsetId& rowset_id, int64_t num_segments) {
    for (int64_t i = 0; i < num_segments; i++) {
        erase_segment(SegmentCache::CacheKey(rowset_id, i));
    }
}

} // namespace doris
