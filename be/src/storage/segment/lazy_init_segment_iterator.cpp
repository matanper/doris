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

#include "storage/segment/lazy_init_segment_iterator.h"

#include <bvar/bvar.h>

#include <typeinfo>

#include "storage/rowset/beta_rowset.h"
#include "storage/segment/segment_loader.h"

namespace doris::segment_v2 {

namespace {
// Segment iterators that reused a segment their reader had pinned instead of loading it.
bvar::Adder<int64_t> g_pinned_segment_reuses("segment_pinned_reuses");
} // namespace

LazyInitSegmentIterator::LazyInitSegmentIterator(BetaRowsetSharedPtr rowset, int64_t segment_id,
                                                 bool should_use_cache, SchemaSPtr schema,
                                                 const StorageReadOptions& opts,
                                                 std::shared_ptr<PinnedSegments> pinned_segments)
        : _rowset(std::move(rowset)),
          _segment_id(segment_id),
          _should_use_cache(should_use_cache),
          _schema(std::move(schema)),
          _read_options(opts),
          _pinned_segments(std::move(pinned_segments)) {}

/// See where the iterator is created in `BetaRowsetReader::get_segment_iterators`
Status LazyInitSegmentIterator::init(const StorageReadOptions& opts) {
    _need_lazy_init = false;
    if (_inner_iterator) {
        return Status::OK();
    }

    // Names the step that throws, since the exception is converted to a Status further up
    // and its stack is lost.
    const char* step = "load_segment";
    try {
        std::shared_ptr<Segment> segment;
        if (_pinned_segments != nullptr) {
            std::lock_guard<std::mutex> lock(_pinned_segments->mutex);
            auto it = _pinned_segments->segments.find(_segment_id);
            if (it != _pinned_segments->segments.end()) {
                segment = it->second;
                g_pinned_segment_reuses << 1;
            }
        }
        if (segment == nullptr) {
            SegmentCacheHandle segment_cache_handle;
            RETURN_IF_ERROR(SegmentLoader::instance()->load_segment(
                    _rowset, _segment_id, &segment_cache_handle, _should_use_cache, false,
                    opts.stats, &opts.io_ctx));
            const auto& tmp_segments = segment_cache_handle.get_segments();
            segment = tmp_segments[0];
            if (_pinned_segments != nullptr) {
                std::lock_guard<std::mutex> lock(_pinned_segments->mutex);
                _pinned_segments->segments.emplace(_segment_id, segment);
            }
        }
        step = "new_iterator";
        RETURN_IF_ERROR(segment->new_iterator(_schema, _read_options, &_inner_iterator));
        step = "inner_iterator_init";
        return _inner_iterator->init(_read_options);
    } catch (const std::exception& e) {
        LOG(WARNING) << "exception initializing segment iterator, rowset=" << _rowset->rowset_id()
                     << " segment=" << _segment_id << " step=" << step << ": "
                     << typeid(e).name() << " " << e.what();
        throw;
    }
}

} // namespace doris::segment_v2
