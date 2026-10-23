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

#include "storage/index/index_file_reader.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/packed_file_system.h"
#include "io/fs/packed_file_trailer.h"
#include "runtime/memory/untracked_heap_account.h"
#include "storage/index/inverted/inverted_index_compound_reader.h"
#include "storage/index/inverted/inverted_index_fs_directory.h"
#include "storage/tablet/tablet_schema.h"
#include "util/debug_points.h"

namespace doris::segment_v2 {

Status IndexFileReader::init(int32_t read_buffer_size, const io::IOContext* io_ctx) {
    std::unique_lock<std::shared_mutex> lock(_mutex); // Lock for writing
    if (!_inited) {
        _read_buffer_size = read_buffer_size;
        if (_storage_format >= InvertedIndexStorageFormatPB::V2) {
            const int64_t allocated_before = thread_net_allocated_bytes();
            RETURN_IF_ERROR(_init_from(read_buffer_size, io_ctx));
            _retained_bytes = std::max<int64_t>(0, thread_net_allocated_bytes() - allocated_before);
        }
        _inited = true;
    }
    return Status::OK();
}

Status IndexFileReader::_init_from(int32_t read_buffer_size, const io::IOContext* io_ctx) {
    auto index_file_full_path = InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix);

    try {
        CLuceneError err;
        CL_NS(store)::IndexInput* index_input = nullptr;

        // 1. get file size from meta
        int64_t file_size = -1;
        if (_idx_file_info.has_index_size()) {
            file_size = _idx_file_info.index_size();
        }
        file_size = file_size == 0 ? -1 : file_size;

        DBUG_EXECUTE_IF("file_size_not_in_rowset_meta ", {
            if (file_size == -1) {
                return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                        "CLuceneError occur file size = -1, file is {}", index_file_full_path);
            }
        })

        DCHECK(_fs != nullptr) << "file system is nullptr, index_file_full_path: "
                               << index_file_full_path;
        // 2. open file
        auto ok =
                DorisFSDirectory::FSIndexInput::open(_fs, index_file_full_path.c_str(), index_input,
                                                     err, read_buffer_size, file_size, _tablet_id);
        if (!ok) {
            if (err.number() == CL_ERR_FileNotFound) {
                return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>(
                        "inverted index file {} is not found.", index_file_full_path);
            } else if (err.number() == CL_ERR_EmptyIndexSegment) {
                return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                        "inverted index file {} is empty.", index_file_full_path);
            }
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "CLuceneError occur when open idx file {}, error msg: {}", index_file_full_path,
                    err.what());
        }
        _stream = std::unique_ptr<CL_NS(store)::IndexInput>(index_input);
        _stream->setIoContext(io_ctx);
        _stream->setIndexFile(true);

        // 3. read file
        int32_t version = _stream->readInt(); // Read version number
        if (version >= InvertedIndexStorageFormatPB::V2) {
            DCHECK(version == _storage_format);
            int32_t numIndices = _stream->readInt(); // Read number of indices

            for (int32_t i = 0; i < numIndices; ++i) {
                int64_t indexId = _stream->readLong();      // Read index ID
                int32_t suffix_length = _stream->readInt(); // Read suffix length
                std::vector<uint8_t> suffix_data(suffix_length);
                _stream->readBytes(suffix_data.data(), suffix_length);
                std::string suffix_str(suffix_data.begin(), suffix_data.end());

                int32_t numFiles = _stream->readInt(); // Read number of files in the index

                auto fileEntries = std::make_unique<EntriesType>();
                fileEntries->reserve(numFiles);

                for (int32_t j = 0; j < numFiles; ++j) {
                    int32_t file_name_length = _stream->readInt();
                    std::string file_name(file_name_length, '\0');
                    _stream->readBytes(reinterpret_cast<uint8_t*>(file_name.data()),
                                       file_name_length);
                    auto entry = std::make_unique<ReaderFileEntry>();
                    entry->file_name = std::move(file_name);
                    entry->offset = _stream->readLong();
                    entry->length = _stream->readLong();
                    fileEntries->emplace(entry->file_name, std::move(entry));
                }

                _indices_entries.emplace(std::make_pair(indexId, std::move(suffix_str)),
                                         std::move(fileEntries));
            }
        } else {
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "unknown inverted index format {}", version);
        }
    } catch (CLuceneError& err) {
        if (_stream != nullptr) {
            try {
                _stream->close();
            } catch (CLuceneError& err) {
                return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                        "CLuceneError occur when close idx file {}, error msg: {}",
                        index_file_full_path, err.what());
            }
        }
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "CLuceneError occur when init idx file {}, error msg: {}", index_file_full_path,
                err.what());
    }
    return Status::OK();
}

Result<InvertedIndexDirectoryMap> IndexFileReader::get_all_directories() {
    InvertedIndexDirectoryMap res;
    std::shared_lock<std::shared_mutex> lock(_mutex); // Lock for reading
    for (auto& [index, _] : _indices_entries) {
        auto&& [index_id, index_suffix] = index;
        LOG(INFO) << "index_id:" << index_id << " index_suffix:" << index_suffix;
        auto ret = _open(index_id, index_suffix);
        if (!ret.has_value()) {
            return ResultError(ret.error());
        }
        res.emplace(std::make_pair(index_id, index_suffix), std::move(ret.value()));
    }
    return res;
}

Result<std::unique_ptr<DorisCompoundReader, DirectoryDeleter>> IndexFileReader::_open(
        int64_t index_id, const std::string& index_suffix, const io::IOContext* io_ctx) const {
    std::unique_ptr<DorisCompoundReader, DirectoryDeleter> compound_reader;

    if (_storage_format == InvertedIndexStorageFormatPB::V1) {
        auto index_file_path = InvertedIndexDescriptor::get_index_file_path_v1(
                _index_path_prefix, index_id, index_suffix);
        try {
            CLuceneError err;
            CL_NS(store)::IndexInput* index_input = nullptr;

            // 1. get file size from meta
            int64_t file_size = -1;
            if (_idx_file_info.index_info_size() > 0) {
                for (const auto& idx_info : _idx_file_info.index_info()) {
                    if (index_id == idx_info.index_id() &&
                        index_suffix == idx_info.index_suffix()) {
                        file_size = idx_info.index_file_size();
                        break;
                    }
                }
            }
            file_size = file_size == 0 ? -1 : file_size;
            DBUG_EXECUTE_IF("file_size_not_in_rowset_meta ", {
                if (file_size == -1) {
                    return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                            "CLuceneError occur file size = -1, file is {}", index_file_path));
                }
            })
            DCHECK(_fs != nullptr)
                    << "file system is nullptr, index_file_path: " << index_file_path;
            // 2. open file
            auto ok = DorisFSDirectory::FSIndexInput::open(_fs, index_file_path.c_str(),
                                                           index_input, err, _read_buffer_size,
                                                           file_size, _tablet_id);
            if (!ok) {
                // now index_input = nullptr
                if (err.number() == CL_ERR_FileNotFound) {
                    return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>(
                            "inverted index file {} is not found.", index_file_path));
                }
                return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                        "CLuceneError occur when open idx file {}, error msg: {}", index_file_path,
                        err.what()));
            }

            // 3. read file in DorisCompoundReader
            compound_reader.reset(new DorisCompoundReader(index_input, _read_buffer_size));
        } catch (CLuceneError& err) {
            return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "CLuceneError occur when open idx file {}, error msg: {}", index_file_path,
                    err.what()));
        }
    } else {
        std::shared_lock<std::shared_mutex> lock(_mutex); // Lock for reading
        if (_stream == nullptr) {
            return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>(
                    "CLuceneError occur when open idx file {}, stream is nullptr",
                    InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix)));
        }

        // Check if the specified index exists
        auto index_it = _indices_entries.find(std::make_pair(index_id, index_suffix));
        if (index_it == _indices_entries.end()) {
            std::ostringstream errMsg;
            errMsg << "No index with id " << index_id << " found";
            return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>(
                    "CLuceneError occur when open idx file {}, error msg: {}",
                    InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix),
                    errMsg.str()));
        }
        // Need to clone resource here, because index searcher cache need it.
        compound_reader.reset(new DorisCompoundReader(_stream->clone(), *index_it->second,
                                                      _read_buffer_size, io_ctx));
    }
    return compound_reader;
}

Result<std::unique_ptr<DorisCompoundReader, DirectoryDeleter>> IndexFileReader::open(
        const TabletIndex* index_meta, const io::IOContext* io_ctx) const {
    auto index_id = index_meta->index_id();
    auto index_suffix = index_meta->get_index_suffix();
    return _open(index_id, index_suffix, io_ctx);
}

std::string IndexFileReader::get_index_file_cache_key(const TabletIndex* index_meta) const {
    return InvertedIndexDescriptor::get_index_file_cache_key(
            _index_path_prefix, index_meta->index_id(), index_meta->get_index_suffix());
}

Status IndexFileReader::read_range_from_storage(const TabletIndex* index_meta, int64_t offset,
                                                int64_t length, std::string* out) const {
    if (_fs == nullptr || offset < 0 || length < 0) {
        return Status::InvalidArgument("cannot read index file range [{}, +{}) from storage",
                                       offset, length);
    }
    io::FileReaderOptions opts;
    opts.cache_type = io::FileCachePolicy::NO_CACHE;
    opts.tablet_id = _tablet_id;
    io::FileReaderSPtr reader;
    RETURN_IF_ERROR(_fs->open_file(get_index_file_path(index_meta), &reader, &opts));
    out->resize(static_cast<size_t>(length));
    size_t bytes_read = 0;
    RETURN_IF_ERROR(reader->read_at(static_cast<size_t>(offset), Slice(out->data(), out->size()),
                                    &bytes_read));
    if (bytes_read != out->size()) {
        return Status::Corruption("short read of index file range [{}, +{}): {} bytes", offset,
                                  length, bytes_read);
    }
    return Status::OK();
}

Status IndexFileReader::packed_slice_locations(const TabletIndex* index_meta,
                                               io::PackedSliceLocation* used,
                                               std::string* used_source,
                                               io::PackedSliceLocation* recorded,
                                               bool* recorded_found) const {
    *recorded_found = false;
    const auto* packed_fs = dynamic_cast<const io::PackedFileSystem*>(_fs.get());
    if (packed_fs == nullptr) {
        return Status::Error<ErrorCode::NOT_FOUND, false>("index file system is not packed");
    }
    const std::string path = get_index_file_path(index_meta);
    const char* source = nullptr;
    if (!packed_fs->find_slice_location(io::Path(path), used, &source)) {
        return Status::Error<ErrorCode::NOT_FOUND, false>("index file {} is not in a packed file",
                                                          path);
    }
    *used_source = source;
    cloud::PackedFileFooterPB footer;
    uint32_t version = 0;
    RETURN_IF_ERROR(io::read_packed_file_trailer(packed_fs->inner_fs(), used->packed_file_path,
                                                 used->packed_file_size, &footer, &version));
    for (const auto& slice : footer.packed_file_info().slices()) {
        if (slice.path() == path) {
            *recorded = *used;
            recorded->offset = slice.offset();
            recorded->size = slice.size();
            *recorded_found = true;
            break;
        }
    }
    return Status::OK();
}

Status IndexFileReader::read_range_at_slice(const io::PackedSliceLocation& slice, int64_t offset,
                                            int64_t length, std::string* out) const {
    const auto* packed_fs = dynamic_cast<const io::PackedFileSystem*>(_fs.get());
    if (packed_fs == nullptr || offset < 0 || length < 0 || offset + length > slice.size) {
        return Status::InvalidArgument("cannot read [{}, +{}) of a {}-byte packed slice", offset,
                                       length, slice.size);
    }
    io::FileReaderOptions opts;
    opts.cache_type = io::FileCachePolicy::NO_CACHE;
    if (slice.packed_file_size > 0) {
        opts.file_size = slice.packed_file_size;
    }
    io::FileReaderSPtr reader;
    RETURN_IF_ERROR(
            packed_fs->inner_fs()->open_file(io::Path(slice.packed_file_path), &reader, &opts));
    out->resize(static_cast<size_t>(length));
    size_t bytes_read = 0;
    RETURN_IF_ERROR(reader->read_at(static_cast<size_t>(slice.offset + offset),
                                    Slice(out->data(), out->size()), &bytes_read));
    if (bytes_read != out->size()) {
        return Status::Corruption("short read of packed slice range [{}, +{}): {} bytes", offset,
                                  length, bytes_read);
    }
    return Status::OK();
}

std::string IndexFileReader::describe_cached_blocks(const TabletIndex* index_meta, int64_t offset,
                                                    int64_t length, bool* any_peer) const {
    *any_peer = false;
    auto* factory = io::FileCacheFactory::instance();
    if (!config::enable_file_cache || factory == nullptr ||
        factory->get_cache_instance_size() == 0 || offset < 0 || length <= 0) {
        return "no file cache";
    }
    const auto hash =
            io::BlockFileCache::hash(io::remote_file_cache_name(get_index_file_path(index_meta)));
    auto* cache = factory->get_by_path(hash);
    if (cache == nullptr) {
        return "no file cache";
    }
    return cache->describe_blocks(hash, static_cast<size_t>(offset),
                                  static_cast<size_t>(offset + length - 1), any_peer);
}

void IndexFileReader::evict_cached_index_file(const TabletIndex* index_meta) const {
    auto* factory = io::FileCacheFactory::instance();
    if (!config::enable_file_cache || factory == nullptr ||
        factory->get_cache_instance_size() == 0) {
        return;
    }
    const auto hash =
            io::BlockFileCache::hash(io::remote_file_cache_name(get_index_file_path(index_meta)));
    if (auto* cache = factory->get_by_path(hash); cache != nullptr) {
        cache->remove_if_cached_async(hash);
    }
}

std::string IndexFileReader::get_index_file_path(const TabletIndex* index_meta) const {
    if (_storage_format == InvertedIndexStorageFormatPB::V1) {
        return InvertedIndexDescriptor::get_index_file_path_v1(
                _index_path_prefix, index_meta->index_id(), index_meta->get_index_suffix());
    }
    return InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix);
}

Status IndexFileReader::index_file_exist(const TabletIndex* index_meta, bool* res) const {
    if (_storage_format == InvertedIndexStorageFormatPB::V1) {
        auto index_file_path = InvertedIndexDescriptor::get_index_file_path_v1(
                _index_path_prefix, index_meta->index_id(), index_meta->get_index_suffix());
        return _fs->exists(index_file_path, res);
    } else {
        std::shared_lock<std::shared_mutex> lock(_mutex); // Lock for reading
        if (_stream == nullptr) {
            *res = false;
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>(
                    "idx file {} is not opened",
                    InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix));
        }
        // Check if the specified index exists
        auto index_it = _indices_entries.find(
                std::make_pair(index_meta->index_id(), index_meta->get_index_suffix()));
        if (index_it == _indices_entries.end()) {
            *res = false;
        } else {
            *res = true;
        }
    }
    return Status::OK();
}

Status IndexFileReader::has_null(const TabletIndex* index_meta, bool* res) const {
    if (_storage_format == InvertedIndexStorageFormatPB::V1) {
        *res = true;
        return Status::OK();
    }
    std::shared_lock<std::shared_mutex> lock(_mutex); // Lock for reading
    if (_stream == nullptr) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>(
                "idx file {} is not opened",
                InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix));
    }
    // Check if the specified index exists
    auto index_it = _indices_entries.find(
            std::make_pair(index_meta->index_id(), index_meta->get_index_suffix()));
    if (index_it == _indices_entries.end()) {
        *res = false;
    } else {
        const auto& entries = index_it->second;
        auto entry_it =
                entries->find(InvertedIndexDescriptor::get_temporary_null_bitmap_file_name());
        if (entry_it == entries->end()) {
            *res = false;
            return Status::OK();
        }
        const auto& e = entry_it->second;
        // roaring bitmap cookie header size is 5
        if (e->length <= 5) {
            *res = false;
        } else {
            *res = true;
        }
    }
    return Status::OK();
}

void IndexFileReader::debug_file_entries() {
    std::shared_lock<std::shared_mutex> lock(_mutex); // Lock for reading
    for (const auto& index : _indices_entries) {
        LOG(INFO) << "index_id:" << index.first.first;
        const auto& index_entries = index.second;
        for (const auto& entry : *index_entries) {
            const auto& file_entry = entry.second;
            LOG(INFO) << "file entry name:" << file_entry->file_name
                      << " length:" << file_entry->length << " offset:" << file_entry->offset;
        }
    }
}

} // namespace doris::segment_v2
