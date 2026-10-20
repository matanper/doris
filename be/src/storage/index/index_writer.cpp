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

#include <bvar/bvar.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <string_view>

#include "common/exception.h"
#include "common/logging.h"
#include "cpp/sync_point.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/file_writer.h"
#include "util/slice.h"
#include "util/time.h"
#include "storage/index/ann/ann_index_writer.h"
#include "storage/index/inverted/inverted_index_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/types.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

// Picks of an index tmp dir that fell back to a store path's tmp dir because every configured
// dir was below index_tmp_file_dir_min_free_bytes.
bvar::Adder<uint64_t> g_index_tmp_file_dir_fallback("index_tmp_file_dir_fallback");
// Entries of the index tmp dirs the orphan sweeper removed, their bytes, and removals that
// failed.
bvar::Adder<uint64_t> g_index_tmp_orphan_dirs_removed("index_tmp_orphan_dirs_removed");
bvar::Adder<uint64_t> g_index_tmp_orphan_bytes_removed("index_tmp_orphan_bytes_removed");
bvar::Adder<uint64_t> g_index_tmp_orphan_remove_failed("index_tmp_orphan_remove_failed");

IndexTmpRegistry& IndexTmpRegistry::instance() {
    static IndexTmpRegistry registry;
    return registry;
}

void IndexTmpRegistry::add(const std::string& prefix) {
    std::lock_guard lock(_mutex);
    ++_prefixes[prefix];
}

void IndexTmpRegistry::remove(const std::string& prefix) {
    std::lock_guard lock(_mutex);
    auto it = _prefixes.find(prefix);
    if (it != _prefixes.end() && --it->second <= 0) {
        _prefixes.erase(it);
    }
}

bool IndexTmpRegistry::covers(const std::string& name) const {
    std::lock_guard lock(_mutex);
    for (const auto& [prefix, count] : _prefixes) {
        if (name.compare(0, prefix.size(), prefix) == 0) {
            return true;
        }
    }
    return false;
}

namespace {

std::string normalized(const std::string& dir) {
    std::error_code ec;
    auto path = std::filesystem::weakly_canonical(std::filesystem::path(dir), ec);
    if (ec) {
        path = std::filesystem::path(dir).lexically_normal();
    }
    std::string out = path.native();
    while (out.size() > 1 && out.back() == '/') {
        out.pop_back();
    }
    return out;
}

// Whether `inner` is `outer` or lies under it.
bool equal_or_under(const std::string& outer, const std::string& inner) {
    return inner == outer || (inner.size() > outer.size() &&
                              inner.compare(0, outer.size(), outer) == 0 &&
                              (outer == "/" || inner[outer.size()] == '/'));
}

bool is_mount_point(const std::string& dir) {
    struct stat self {};
    struct stat parent {};
    if (::stat(dir.c_str(), &self) != 0) {
        return false;
    }
    const std::string up = std::filesystem::path(dir).parent_path().native();
    if (::stat(up.c_str(), &parent) != 0) {
        return false;
    }
    return self.st_dev != parent.st_dev || self.st_ino == parent.st_ino;
}

std::vector<std::string> split_dirs(const std::string& value) {
    std::vector<std::string> dirs;
    size_t start = 0;
    while (start <= value.size()) {
        size_t end = value.find(',', start);
        if (end == std::string::npos) {
            end = value.size();
        }
        std::string item = value.substr(start, end - start);
        const auto first = item.find_first_not_of(" \t");
        const auto last = item.find_last_not_of(" \t");
        if (first != std::string::npos) {
            dirs.push_back(item.substr(first, last - first + 1));
        }
        start = end + 1;
    }
    return dirs;
}

std::vector<std::string> protected_dirs() {
    std::vector<std::string> dirs;
    const char* home = std::getenv("DORIS_HOME");
    if (!config::sys_log_dir.empty()) {
        dirs.push_back(config::sys_log_dir);
    } else if (const char* log_dir = std::getenv("LOG_DIR"); log_dir != nullptr && *log_dir) {
        dirs.emplace_back(log_dir);
    } else if (home != nullptr && *home) {
        dirs.push_back(std::string(home) + "/log");
    }
    if (home != nullptr && *home) {
        dirs.push_back(std::string(home) + "/conf");
    }
    return dirs;
}

} // namespace

TmpFileDirs::TmpFileDirs(const std::vector<doris::StorePath>& store_paths) {
    for (const auto& store_path : store_paths) {
        _tmp_file_dirs.emplace_back(store_path.path + "/" + config::tmp_file_dir);
    }
}

TmpFileDirs::~TmpFileDirs() {
    {
        std::lock_guard lock(_sweeper_mutex);
        _sweeper_stop = true;
    }
    _sweeper_cv.notify_all();
    if (_sweeper.joinable()) {
        _sweeper.join();
    }
}

void TmpFileDirs::start_orphan_sweeper() {
    if (_sweeper.joinable()) {
        return;
    }
    _sweeper = std::thread([this]() {
        pthread_setname_np(pthread_self(), "IndexTmpSweeper");
        for (;;) {
            {
                std::unique_lock lock(_sweeper_mutex);
                const int interval = std::max(1, config::index_tmp_orphan_sweep_interval_seconds);
                _sweeper_cv.wait_for(lock, std::chrono::seconds(interval),
                                     [&]() { return _sweeper_stop; });
                if (_sweeper_stop) {
                    return;
                }
            }
            static_cast<void>(sweep_orphans());
        }
    });
}

size_t TmpFileDirs::sweep_orphans() {
    const int64_t max_age_minutes = config::index_tmp_orphan_max_age_minutes;
    if (max_age_minutes <= 0) {
        return 0;
    }
    const auto now = std::filesystem::file_time_type::clock::now();
    const auto max_age = std::chrono::minutes(max_age_minutes);
    std::vector<io::Path> roots = _index_tmp_dirs;
    roots.insert(roots.end(), _tmp_file_dirs.begin(), _tmp_file_dirs.end());
    size_t removed = 0;
    for (const auto& root : roots) {
        std::error_code ec;
        std::vector<std::filesystem::directory_entry> entries;
        for (std::filesystem::directory_iterator it(root, ec), end; !ec && it != end;
             it.increment(ec)) {
            entries.push_back(*it);
        }
        if (ec) {
            LOG(WARNING) << "index tmp orphan sweep cannot list " << root.native() << ": "
                         << ec.message();
            continue;
        }
        TEST_SYNC_POINT_CALLBACK("TmpFileDirs::sweep_orphans:listed", &root);
        for (const auto& entry : entries) {
            const std::string name = entry.path().filename().native();
            std::error_code entry_ec;
            if (entry.is_symlink(entry_ec) || entry_ec ||
                IndexTmpRegistry::instance().covers(name)) {
                continue;
            }
            // The newest change under the entry, and its bytes; links are not followed.
            auto newest = std::filesystem::last_write_time(entry.path(), entry_ec);
            if (entry_ec) {
                continue;
            }
            uintmax_t bytes = 0;
            if (entry.is_directory(entry_ec) && !entry_ec) {
                for (std::filesystem::recursive_directory_iterator it(
                             entry.path(), std::filesystem::directory_options::none, entry_ec),
                     end;
                     !entry_ec && it != end; it.increment(entry_ec)) {
                    std::error_code file_ec;
                    const auto mtime = std::filesystem::last_write_time(it->path(), file_ec);
                    if (!file_ec && mtime > newest) {
                        newest = mtime;
                    }
                    if (it->is_regular_file(file_ec) && !file_ec) {
                        bytes += it->file_size(file_ec);
                    }
                }
                if (entry_ec) {
                    // Changed or gone while being walked: not idle.
                    continue;
                }
            } else {
                bytes = entry.file_size(entry_ec);
            }
            if (now - newest < max_age) {
                continue;
            }
            // A writer may have registered since the listing.
            if (IndexTmpRegistry::instance().covers(name)) {
                continue;
            }
            std::error_code remove_ec;
            std::filesystem::remove_all(entry.path(), remove_ec);
            if (remove_ec) {
                g_index_tmp_orphan_remove_failed << 1;
                LOG(WARNING) << "index tmp orphan sweep cannot remove " << entry.path().native()
                             << ": " << remove_ec.message();
                continue;
            }
            ++removed;
            g_index_tmp_orphan_dirs_removed << 1;
            g_index_tmp_orphan_bytes_removed << bytes;
            LOG(INFO) << "index tmp orphan sweep removed " << entry.path().native() << ", "
                      << bytes << " bytes";
        }
    }
    return removed;
}

Status TmpFileDirs::validate_index_tmp_dir(const std::string& dir,
                                           const std::vector<std::string>& store_paths,
                                           const std::vector<std::string>& cache_paths,
                                           const std::vector<std::string>& protected_paths) {
    if (!std::filesystem::path(dir).is_absolute()) {
        return Status::InvalidArgument("index_tmp_file_dirs: {} is not an absolute path", dir);
    }
    const std::string path = normalized(dir);
    if (path == "/") {
        return Status::InvalidArgument("index_tmp_file_dirs: refusing /");
    }
    for (const auto& store_path : store_paths) {
        if (equal_or_under(path, normalized(store_path))) {
            return Status::InvalidArgument(
                    "index_tmp_file_dirs: {} equals or contains storage_root_path {}", dir,
                    store_path);
        }
    }
    for (const auto& cache_path : cache_paths) {
        const std::string cache = normalized(cache_path);
        if (path == cache + "/" + io::FILE_CACHE_INDEX_TMP_DIR_NAME) {
            continue;
        }
        if (equal_or_under(path, cache) || equal_or_under(cache, path)) {
            return Status::InvalidArgument(
                    "index_tmp_file_dirs: {} equals, contains or is inside file_cache_path {}; "
                    "only {}/{} is allowed there",
                    dir, cache_path, cache_path, io::FILE_CACHE_INDEX_TMP_DIR_NAME);
        }
    }
    for (const auto& protected_dir : protected_paths) {
        const std::string other = normalized(protected_dir);
        if (equal_or_under(path, other) || equal_or_under(other, path)) {
            return Status::InvalidArgument(
                    "index_tmp_file_dirs: {} equals, contains or is inside {}", dir,
                    protected_dir);
        }
    }
    if (std::filesystem::exists(path) && is_mount_point(path)) {
        return Status::InvalidArgument("index_tmp_file_dirs: {} is a mount point", dir);
    }
    return Status::OK();
}

Status TmpFileDirs::init() {
    for (auto& tmp_file_dir : _tmp_file_dirs) {
        // delete the tmp dir to avoid the tmp files left by last crash
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_directory(tmp_file_dir));
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(tmp_file_dir));
    }
    _index_tmp_dirs.clear();
    const auto dirs = split_dirs(config::index_tmp_file_dirs);
    if (dirs.empty()) {
        return Status::OK();
    }
    std::vector<std::string> store_paths;
    for (const auto& tmp_file_dir : _tmp_file_dirs) {
        store_paths.push_back(tmp_file_dir.parent_path().native());
    }
    std::vector<std::string> cache_paths;
    if (!config::file_cache_path.empty()) {
        std::vector<CachePath> parsed;
        if (parse_conf_cache_paths(config::file_cache_path, parsed).ok()) {
            for (const auto& cache : parsed) {
                cache_paths.push_back(cache.path);
            }
        }
    }
    const auto protected_list = protected_dirs();
    // Every dir is checked before any is emptied.
    for (const auto& dir : dirs) {
        RETURN_IF_ERROR(validate_index_tmp_dir(dir, store_paths, cache_paths, protected_list));
    }
    for (const auto& dir : dirs) {
        const io::Path path(normalized(dir));
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_directory(path));
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(path));
        const io::Path probe = path / ".write_probe";
        io::FileWriterPtr writer;
        Status st = io::global_local_filesystem()->create_file(probe, &writer);
        if (st.ok()) {
            st = writer->append(Slice("x", 1));
        }
        if (st.ok()) {
            st = writer->close();
        }
        if (st.ok()) {
            st = io::global_local_filesystem()->delete_file(probe);
        }
        if (!st.ok()) {
            return Status::IOError("index_tmp_file_dirs: {} is not writable: {}", dir,
                                   st.to_string());
        }
        _index_tmp_dirs.push_back(path);
        LOG(INFO) << "index tmp file dir " << path.native();
    }
    _free = std::make_unique<std::atomic<int64_t>[]>(_index_tmp_dirs.size());
    _free_read_ms = std::make_unique<std::atomic<int64_t>[]>(_index_tmp_dirs.size());
    for (size_t i = 0; i < _index_tmp_dirs.size(); ++i) {
        _free[i] = INT64_MAX;
        _free_read_ms[i] = 0;
    }
    return Status::OK();
}

int64_t TmpFileDirs::free_bytes(size_t index) {
    const int64_t now_ms = MonotonicMillis();
    // Free space is read at most once a second per dir.
    if (now_ms - _free_read_ms[index].load(std::memory_order_relaxed) >= 1000) {
        _free_read_ms[index].store(now_ms, std::memory_order_relaxed);
        int64_t bytes = INT64_MAX;
        if (_free_bytes_fn) {
            bytes = _free_bytes_fn(_index_tmp_dirs[index]);
        } else {
            struct statvfs vfs {};
            if (::statvfs(_index_tmp_dirs[index].c_str(), &vfs) == 0) {
                bytes = static_cast<int64_t>(vfs.f_bavail) * static_cast<int64_t>(vfs.f_frsize);
            }
        }
        _free[index].store(bytes, std::memory_order_relaxed);
    }
    return _free[index].load(std::memory_order_relaxed);
}

io::Path TmpFileDirs::get_tmp_file_dir() {
    if (!_index_tmp_dirs.empty()) {
        const int64_t min_free = config::index_tmp_file_dir_min_free_bytes;
        const size_t start = _next_index_tmp.fetch_add(1);
        for (size_t i = 0; i < _index_tmp_dirs.size(); ++i) {
            const size_t index = (start + i) % _index_tmp_dirs.size();
            if (min_free <= 0 || free_bytes(index) >= min_free) {
                return _index_tmp_dirs[index];
            }
        }
        g_index_tmp_file_dir_fallback << 1;
        LOG_EVERY_N(WARNING, 100) << "every index_tmp_file_dirs entry has less than "
                                  << min_free << " bytes free; using a store path's tmp dir";
    }
    size_t cur_index = _next_index.fetch_add(1);
    return _tmp_file_dirs[cur_index % _tmp_file_dirs.size()];
}

bool IndexColumnWriter::check_support_inverted_index(const TabletColumn& column) {
    // bellow types are not supported in inverted index for extracted columns
    static std::set<FieldType> invalid_types = {FieldType::OLAP_FIELD_TYPE_JSONB};
    if (invalid_types.contains(column.type())) {
        return false;
    }
    if (column.is_variant_type()) {
        return false;
    }
    if (column.is_array_type()) {
        // only support one level array
        const auto& subcolumn = column.get_sub_column(0);
        return !subcolumn.is_array_type() && check_support_inverted_index(subcolumn);
    }
    return true;
}

bool IndexColumnWriter::check_support_ann_index(const TabletColumn& column) {
    // only array are supported in ann index
    return column.is_array_type();
}

// create index writer
Status IndexColumnWriter::create(const TabletColumn* column,
                                 std::unique_ptr<IndexColumnWriter>* res,
                                 IndexFileWriter* index_file_writer,
                                 const TabletIndex* index_meta) {
    FieldType type = column->type();
    std::string field_name;
    auto storage_format = index_file_writer->get_storage_format();
    if (storage_format == InvertedIndexStorageFormatPB::V1) {
        field_name = column->name();
    } else {
        if (column->is_extracted_column()) {
            // variant sub col
            // field_name format: parent_unique_id.sub_col_name
            field_name = std::to_string(column->parent_unique_id()) + "." + column->name();
        } else {
            field_name = std::to_string(column->unique_id());
        }
    }

    if (index_meta->is_inverted_index()) {
        bool single_field = true;
        if (type == FieldType::OLAP_FIELD_TYPE_ARRAY) {
            bool has_item_subcolumn = column->get_subtype_count() > 0;
            DBUG_EXECUTE_IF("InvertedIndexColumnWriter::create_array_typeinfo_is_nullptr",
                            { has_item_subcolumn = false; })
            if (has_item_subcolumn) {
                type = column->get_sub_column(0).type();
                single_field = false;
            } else {
                return Status::NotSupported("unsupported array type for inverted index: " +
                                            std::to_string(int(type)));
            }
        }

        DBUG_EXECUTE_IF("InvertedIndexColumnWriter::create_unsupported_type_for_inverted_index",
                        { type = FieldType::OLAP_FIELD_TYPE_JSONB; })
        switch (type) {
#define M(TYPE)                                                                                 \
    case TYPE:                                                                                  \
        *res = std::make_unique<InvertedIndexColumnWriter<TYPE>>(field_name, index_file_writer, \
                                                                 index_meta, single_field);     \
        break;
            M(FieldType::OLAP_FIELD_TYPE_TINYINT)
            M(FieldType::OLAP_FIELD_TYPE_SMALLINT)
            M(FieldType::OLAP_FIELD_TYPE_INT)
            M(FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT)
            M(FieldType::OLAP_FIELD_TYPE_BIGINT)
            M(FieldType::OLAP_FIELD_TYPE_LARGEINT)
            M(FieldType::OLAP_FIELD_TYPE_CHAR)
            M(FieldType::OLAP_FIELD_TYPE_VARCHAR)
            M(FieldType::OLAP_FIELD_TYPE_STRING)
            M(FieldType::OLAP_FIELD_TYPE_DATE)
            M(FieldType::OLAP_FIELD_TYPE_DATETIME)
            M(FieldType::OLAP_FIELD_TYPE_DECIMAL)
            M(FieldType::OLAP_FIELD_TYPE_DATEV2)
            M(FieldType::OLAP_FIELD_TYPE_DATETIMEV2)
            M(FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ)
            M(FieldType::OLAP_FIELD_TYPE_DECIMAL32)
            M(FieldType::OLAP_FIELD_TYPE_DECIMAL64)
            M(FieldType::OLAP_FIELD_TYPE_DECIMAL128I)
            M(FieldType::OLAP_FIELD_TYPE_DECIMAL256)
            M(FieldType::OLAP_FIELD_TYPE_BOOL)
            M(FieldType::OLAP_FIELD_TYPE_IPV4)
            M(FieldType::OLAP_FIELD_TYPE_IPV6)
            M(FieldType::OLAP_FIELD_TYPE_FLOAT)
            M(FieldType::OLAP_FIELD_TYPE_DOUBLE)
#undef M
        default:
            return Status::NotSupported("unsupported type for inverted index: " +
                                        std::to_string(int(type)));
        }
        if (*res != nullptr) {
            auto st = (*res)->init();
            if (!st.ok()) {
                (*res)->close_on_error();
                return st;
            }
        }
    } else if (index_meta->is_ann_index()) {
        DCHECK(type == FieldType::OLAP_FIELD_TYPE_ARRAY);
        *res = std ::make_unique<AnnIndexColumnWriter>(index_file_writer, index_meta);
        if (*res != nullptr) {
            auto st = (*res)->init();
            if (!st.ok()) {
                (*res)->close_on_error();
                return st;
            }
        }
    }

    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2
