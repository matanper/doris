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

#include <benchmark/benchmark.h>
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <time.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_finish_batch.h"
#include "common/config.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/options.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

// Builds one inverted index of each kind melian.logs carries, over log-like values, through the
// writer a segment flush or a compaction uses: CPU per row of adding the values and of finishing
// the index, and the size of the index file. INDEX_BUILD_DATA_DIR, when set, names a directory of
// <kind>.txt files with one value per line to use instead of the generated values.
namespace doris::inverted_index_build_bench {

template <typename Tag, typename Tag::type Member>
struct PrivateMemberAccess {
    friend typename Tag::type private_member(Tag) { return Member; }
};
struct ExecEnvIndexPolicyMgrTag {
    using type = IndexPolicyMgr* ExecEnv::*;
    friend type private_member(ExecEnvIndexPolicyMgrTag);
};
template struct PrivateMemberAccess<ExecEnvIndexPolicyMgrTag, &ExecEnv::_index_policy_mgr>;

inline void install_analyzers() {
    static IndexPolicyMgr* mgr = [] {
        auto* m = new IndexPolicyMgr();
        TIndexPolicy alnum;
        alnum.id = 1;
        alnum.name = "alnum_lower";
        alnum.type = TIndexPolicyType::ANALYZER;
        alnum.properties["tokenizer"] = "basic";
        alnum.properties["token_filter"] = "lowercase";
        TIndexPolicy ngram;
        ngram.id = 2;
        ngram.name = "ngram4";
        ngram.type = TIndexPolicyType::TOKENIZER;
        ngram.properties["type"] = "ngram";
        ngram.properties["min_gram"] = "4";
        ngram.properties["max_gram"] = "4";
        TIndexPolicy ngram_lower;
        ngram_lower.id = 3;
        ngram_lower.name = "ngram4_lower";
        ngram_lower.type = TIndexPolicyType::ANALYZER;
        ngram_lower.properties["tokenizer"] = "ngram4";
        ngram_lower.properties["token_filter"] = "lowercase";
        m->apply_policy_changes({alnum, ngram, ngram_lower}, {});
        ExecEnv::GetInstance()->*private_member(ExecEnvIndexPolicyMgrTag {}) = m;
        std::vector<StorePath> paths;
        paths.emplace_back("/tmp/inverted_index_build_bench_store", -1);
        auto tmp_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        static_cast<void>(tmp_dirs->init());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_dirs));
        return m;
    }();
    (void)mgr;
}

enum Kind {
    EQ_LOW_CARD,   // class_name, status, action ...: a few dozen values
    EQ_HIGH_CARD,  // src_endpoint.ip, actor.user.uid ...: mostly distinct
    EQ_BIGINT,     // class_uid, dst_endpoint.port ...: BKD
    RAW_ALNUM,     // raw: alnum_lower, no phrase
    MESSAGE_ALNUM, // message: alnum_lower with phrase
    CMD_NGRAM,     // process.cmd_line: ngram4_lower with phrase
    PATH_NGRAM,    // file.path, process.file.path: ngram4_lower with phrase
    NAME_NGRAM,    // device.hostname, process.name, actor.user.name: ngram4_lower with phrase
};

inline const char* kind_name(int kind) {
    static const char* names[] = {"eq_low_card", "eq_high_card", "eq_bigint", "raw_alnum",
                                  "message_alnum", "cmd_ngram", "path_ngram", "name_ngram"};
    return names[kind];
}

inline std::string hex(uint64_t v, int digits) {
    static const char* d = "0123456789abcdef";
    std::string s(digits, '0');
    for (int i = digits - 1; i >= 0; --i, v >>= 4) {
        s[i] = d[v & 15];
    }
    return s;
}

inline std::vector<std::string> generate(int kind, size_t rows) {
    std::mt19937_64 rng(42 + kind);
    auto pick = [&](const std::vector<std::string>& v) { return v[rng() % v.size()]; };
    static const std::vector<std::string> users = {"alice.smith", "bob.jones", "svc_backup",
                                                   "SYSTEM", "Administrator", "carol.wu",
                                                   "dave.o'neil", "eve-ops", "mallory"};
    static const std::vector<std::string> hosts = {"ip-10-20-30-40.ec2.internal", "WIN-7H3K2L9",
                                                   "prod-web-01", "DC01.corp.example.com",
                                                   "laptop-JSMITH", "k8s-node-pool-a-7f9c"};
    static const std::vector<std::string> exes = {"cmd.exe", "powershell.exe", "svchost.exe",
                                                  "chrome.exe", "python3", "sshd", "bash",
                                                  "rundll32.exe", "msiexec.exe", "node"};
    static const std::vector<std::string> dirs = {"C:\\Windows\\System32\\",
                                                  "C:\\Program Files\\Google\\Chrome\\Application\\",
                                                  "/usr/bin/", "/opt/app/releases/2026.10.14/bin/",
                                                  "C:\\Users\\jsmith\\AppData\\Local\\Temp\\",
                                                  "/var/lib/docker/overlay2/"};
    static const std::vector<std::string> words = {
            "user", "logged", "in", "failed", "password", "for", "from", "port", "ssh2",
            "accepted", "publickey", "session", "opened", "closed", "connection", "reset", "by",
            "peer", "GET", "POST", "HTTP/1.1", "200", "403", "500", "AssumeRole", "PutObject",
            "DescribeInstances", "ConsoleLogin", "Success", "Failure", "AccessDenied", "token",
            "expired", "policy", "violation", "detected", "malware", "quarantined", "process",
            "created", "terminated", "network", "allowed", "blocked", "dns", "query", "NXDOMAIN"};
    std::vector<std::string> out;
    out.reserve(rows);
    for (size_t i = 0; i < rows; ++i) {
        std::string v;
        switch (kind) {
        case EQ_LOW_CARD:
            v = "Class_" + std::to_string(rng() % 40);
            break;
        case EQ_HIGH_CARD:
            v = std::to_string(10 + rng() % 200) + "." + std::to_string(rng() % 256) + "." +
                std::to_string(rng() % 256) + "." + std::to_string(rng() % 256);
            break;
        case RAW_ALNUM: {
            // A ~1.2 KB JSON event, as pipelines receive them.
            v = R"({"eventVersion":"1.08","userIdentity":{"type":"AssumedRole","principalId":"AROA)" +
                hex(rng(), 16) + R"(:)" + pick(users) + R"(","arn":"arn:aws:sts::)" +
                std::to_string(100000000000ULL + rng() % 900000000000ULL) +
                R"(:assumed-role/)" + pick(users) + R"(","accountId":")" +
                std::to_string(100000000000ULL + rng() % 900000000000ULL) +
                R"("},"eventTime":"2026-10-14T08:)" + std::to_string(rng() % 60) + ":" +
                std::to_string(rng() % 60) + R"(Z","eventSource":"s3.amazonaws.com","eventName":")" +
                pick(words) + R"(","awsRegion":"us-east-1","sourceIPAddress":")" +
                std::to_string(rng() % 256) + "." + std::to_string(rng() % 256) + "." +
                std::to_string(rng() % 256) + "." + std::to_string(rng() % 256) +
                R"(","userAgent":"aws-sdk-go-v2/1.30.3 os/linux lang/go#1.22.5 md/GOOS#linux",)" +
                R"("requestParameters":{"bucketName":"prod-logs-)" + hex(rng(), 8) +
                R"(","key":"tenant/)" + hex(rng(), 12) + "/" + pick(words) + "/" + hex(rng(), 24) +
                R"(.json.gz"},"responseElements":null,"requestID":")" + hex(rng(), 16) +
                R"(","eventID":")" + hex(rng(), 8) + "-" + hex(rng(), 4) + "-" + hex(rng(), 4) +
                "-" + hex(rng(), 12) + R"(","readOnly":false,"resources":[{"type":"AWS::S3::Object","ARN":"arn:aws:s3:::prod-logs/)" +
                hex(rng(), 32) + R"("}],"eventType":"AwsApiCall","managementEvent":false,)" +
                R"("recipientAccountId":")" +
                std::to_string(100000000000ULL + rng() % 900000000000ULL) +
                R"(","eventCategory":"Data","tlsDetails":{"tlsVersion":"TLSv1.3",)" +
                R"("cipherSuite":"TLS_AES_128_GCM_SHA256","clientProvidedHostHeader":"prod-logs.s3.us-east-1.amazonaws.com"}})";
            break;
        }
        case MESSAGE_ALNUM:
            for (int w = 0, n = 8 + rng() % 16; w < n; ++w) {
                v += (w ? " " : "") + (rng() % 5 == 0 ? hex(rng(), 8) : pick(words));
            }
            break;
        case CMD_NGRAM:
            v = pick(dirs) + pick(exes) + " /c " + pick(words) + " --user=" + pick(users) +
                " -EncodedCommand " + hex(rng(), 16) + hex(rng(), 16) + " " + pick(dirs) +
                hex(rng(), 6) + ".tmp";
            break;
        case PATH_NGRAM:
            v = pick(dirs) + hex(rng(), 6 + rng() % 10) + "\\" + pick(exes);
            break;
        case NAME_NGRAM:
            v = rng() % 2 ? pick(hosts) + std::to_string(rng() % 500) : pick(users) + std::to_string(rng() % 2000);
            break;
        default:
            break;
        }
        out.push_back(std::move(v));
    }
    return out;
}

inline std::vector<std::string> values(int kind, size_t rows) {
    if (const char* dir = std::getenv("INDEX_BUILD_DATA_DIR")) {
        std::ifstream in(std::string(dir) + "/" + kind_name(kind) + ".txt");
        std::vector<std::string> out;
        std::string line;
        while (out.size() < rows && std::getline(in, line)) {
            out.push_back(line);
        }
        if (!out.empty()) {
            for (size_t i = 0; out.size() < rows; ++i) {
                out.push_back(out[i]);
            }
            return out;
        }
    }
    return generate(kind, rows);
}

inline int64_t thread_cpu_ns() {
    timespec ts;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

inline TabletIndex index_meta(int kind, int64_t index_id = -1) {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::INVERTED);
    pb.set_index_id(index_id >= 0 ? index_id : 10000 + kind);
    pb.set_index_name(kind_name(kind));
    pb.add_col_unique_id(index_id >= 0 ? static_cast<int32_t>(index_id) : 1);
    auto* p = pb.mutable_properties();
    switch (kind) {
    case RAW_ALNUM:
        (*p)["analyzer"] = "alnum_lower";
        (*p)["support_phrase"] = "false";
        break;
    case MESSAGE_ALNUM:
        (*p)["analyzer"] = "alnum_lower";
        (*p)["support_phrase"] = "true";
        break;
    case CMD_NGRAM:
    case PATH_NGRAM:
    case NAME_NGRAM:
        (*p)["analyzer"] = "ngram4_lower";
        (*p)["support_phrase"] = "true";
        break;
    default:
        break;
    }
    TabletIndex meta;
    meta.init_from_pb(pb);
    return meta;
}

// Builds the index of `kind` over `rows` values, adding them in blocks of 4096 rows as a segment
// writer does. Returns the index file's size.
inline int64_t build(int kind, const std::vector<std::string>& strings,
                     const std::vector<int64_t>& ints, int64_t* add_ns, int64_t* finish_ns,
                     const std::string& dir) {
    install_analyzers();
    TabletColumn column;
    column.set_name("c");
    column.set_unique_id(1);
    column.set_is_nullable(false);
    if (kind == EQ_BIGINT) {
        column.set_type(FieldType::OLAP_FIELD_TYPE_BIGINT);
        column.set_length(8);
    } else {
        column.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
        column.set_length(INT32_MAX);
    }
    auto meta = index_meta(kind);
    auto fs = io::global_local_filesystem();
    std::string prefix = dir + "/" + kind_name(kind);
    std::string path = InvertedIndexDescriptor::get_index_file_path_v2(prefix);
    io::FileWriterPtr file_writer;
    if (!fs->create_file(path, &file_writer).ok()) {
        return -1;
    }
    auto index_file_writer = std::make_unique<segment_v2::IndexFileWriter>(
            fs, prefix, "bench", 0, InvertedIndexStorageFormatPB::V3, std::move(file_writer));
    std::unique_ptr<segment_v2::IndexColumnWriter> writer;
    if (!segment_v2::IndexColumnWriter::create(&column, &writer, index_file_writer.get(), &meta)
                 .ok()) {
        return -1;
    }
    const size_t rows = kind == EQ_BIGINT ? ints.size() : strings.size();
    std::vector<Slice> slices;
    slices.reserve(rows);
    for (const auto& s : strings) {
        slices.emplace_back(s.data(), s.size());
    }
    int64_t start = thread_cpu_ns();
    for (size_t off = 0; off < rows; off += 4096) {
        const size_t n = std::min<size_t>(4096, rows - off);
        Status st = kind == EQ_BIGINT ? writer->add_values("c", ints.data() + off, n)
                                      : writer->add_values("c", slices.data() + off, n);
        if (!st.ok()) {
            return -1;
        }
    }
    int64_t mid = thread_cpu_ns();
    if (!writer->finish().ok() || !index_file_writer->begin_close().ok() ||
        !index_file_writer->finish_close().ok()) {
        return -1;
    }
    *add_ns += mid - start;
    *finish_ns += thread_cpu_ns() - mid;
    int64_t size = 0;
    static_cast<void>(fs->file_size(path, &size));
    return size;
}

// FNV-1a of the index file, so that two builds can be checked to write the same bytes.
inline uint32_t file_digest(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    uint64_t h = 1469598103934665603ULL;
    char buf[1 << 16];
    while (in.read(buf, sizeof(buf)) || in.gcount() > 0) {
        for (std::streamsize i = 0; i < in.gcount(); ++i) {
            h = (h ^ static_cast<uint8_t>(buf[i])) * 1099511628211ULL;
        }
    }
    return static_cast<uint32_t>(h ^ (h >> 32));
}

static void IndexBuild(benchmark::State& state) {
    const int kind = static_cast<int>(state.range(0));
    const auto rows = static_cast<size_t>(state.range(1));
    std::vector<std::string> strings;
    std::vector<int64_t> ints;
    if (kind == EQ_BIGINT) {
        std::mt19937_64 rng(7);
        for (size_t i = 0; i < rows; ++i) {
            ints.push_back(static_cast<int64_t>(rng() % 70000));
        }
    } else {
        strings = values(kind, rows);
    }
    std::string dir = "/tmp/inverted_index_build_bench";
    std::filesystem::create_directories(dir);
    int64_t add_ns = 0;
    int64_t finish_ns = 0;
    int64_t bytes = 0;
    for (auto _ : state) {
        bytes = build(kind, strings, ints, &add_ns, &finish_ns, dir);
        if (bytes < 0) {
            state.SkipWithError("index build failed");
            return;
        }
    }
    const double total_rows = static_cast<double>(rows) * static_cast<double>(state.iterations());
    state.counters["add_ns_per_row"] = static_cast<double>(add_ns) / total_rows;
    state.counters["finish_ns_per_row"] = static_cast<double>(finish_ns) / total_rows;
    state.counters["index_bytes"] = static_cast<double>(bytes);

    char digest[16];
    snprintf(digest, sizeof(digest), "%08x", file_digest(InvertedIndexDescriptor::get_index_file_path_v2(
                                                     dir + "/" + kind_name(kind))));
    state.SetLabel(std::string(kind_name(kind)) + " " + digest);
}

BENCHMARK(IndexBuild)
        ->ArgsProduct({{EQ_LOW_CARD, EQ_HIGH_CARD, EQ_BIGINT, RAW_ALNUM, MESSAGE_ALNUM, CMD_NGRAM,
                        PATH_NGRAM, NAME_NGRAM},
                       {200000}})
        ->Unit(benchmark::kMillisecond)
        ->MeasureProcessCPUTime()
        ->UseRealTime();

// One segment's worth of indexes, as melian.logs writes: `copies` writers of every text kind
// and of the eq kinds, all into one index file, finished with index_finish_parallelism =
// range(0). Reports the wall time of the finish step and the file's digest.
static void IndexBuildSegment(benchmark::State& state) {
    install_analyzers();
    const int parallelism = static_cast<int>(state.range(0));
    const size_t rows = static_cast<size_t>(state.range(1));
    const int copies = 4;
    std::vector<int> kinds;
    for (int c = 0; c < copies; ++c) {
        for (int k : {EQ_LOW_CARD, EQ_HIGH_CARD, RAW_ALNUM, MESSAGE_ALNUM, CMD_NGRAM, PATH_NGRAM,
                      NAME_NGRAM}) {
            if (k == RAW_ALNUM && c > 0) {
                continue;
            }
            kinds.push_back(k);
        }
    }
    std::vector<std::vector<std::string>> data(8);
    for (int k : kinds) {
        if (data[k].empty()) {
            data[k] = values(k, rows);
        }
    }
    std::string dir = "/tmp/inverted_index_build_bench";
    std::filesystem::create_directories(dir);
    const int old_parallelism = config::index_finish_parallelism;
    config::index_finish_parallelism = parallelism;
    int64_t finish_wall_ns = 0;
    std::string prefix = dir + "/segment";
    for (auto _ : state) {
        auto fs = io::global_local_filesystem();
        std::string path = InvertedIndexDescriptor::get_index_file_path_v2(prefix);
        io::FileWriterPtr file_writer;
        if (!fs->create_file(path, &file_writer).ok()) {
            state.SkipWithError("create failed");
            break;
        }
        auto index_file_writer = std::make_unique<segment_v2::IndexFileWriter>(
                fs, prefix, "bench", 0, InvertedIndexStorageFormatPB::V3, std::move(file_writer));
        std::vector<TabletColumn> columns(kinds.size());
        std::vector<TabletIndex> metas;
        metas.reserve(kinds.size());
        std::vector<std::unique_ptr<segment_v2::IndexColumnWriter>> writers(kinds.size());
        for (size_t i = 0; i < kinds.size(); ++i) {
            columns[i].set_name("c" + std::to_string(i));
            columns[i].set_unique_id(static_cast<int32_t>(i + 1));
            columns[i].set_is_nullable(false);
            columns[i].set_type(FieldType::OLAP_FIELD_TYPE_STRING);
            columns[i].set_length(INT32_MAX);
            metas.push_back(index_meta(kinds[i], static_cast<int64_t>(i + 1)));
            if (!segment_v2::IndexColumnWriter::create(&columns[i], &writers[i],
                                                       index_file_writer.get(), &metas[i])
                         .ok()) {
                state.SkipWithError("create writer failed");
                return;
            }
            const auto& strings = data[kinds[i]];
            std::vector<Slice> slices;
            slices.reserve(strings.size());
            for (const auto& v : strings) {
                slices.emplace_back(v.data(), v.size());
            }
            for (size_t off = 0; off < slices.size(); off += 4096) {
                static_cast<void>(writers[i]->add_values(columns[i].name(), slices.data() + off,
                                                         std::min<size_t>(4096, slices.size() - off)));
            }
        }
        auto start = std::chrono::steady_clock::now();
        if (parallelism == 0) {
            std::string per;
            for (size_t i = 0; i < writers.size(); ++i) {
                auto t0 = std::chrono::steady_clock::now();
                static_cast<void>(writers[i]->finish());
                per += std::to_string(kinds[i]) + ":" +
                       std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                                              std::chrono::steady_clock::now() - t0)
                                              .count()) +
                       " ";
            }
            LOG(WARNING) << "per-writer finish ms: " << per;
        } else {
            segment_v2::IndexFinishBatch batch;
            for (auto& w : writers) {
                static_cast<void>(segment_v2::IndexFinishBatch::finish_index(w.get()));
            }
            if (!batch.run().ok()) {
                state.SkipWithError("finish failed");
                break;
            }
        }
        finish_wall_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                                  std::chrono::steady_clock::now() - start)
                                  .count();
        static_cast<void>(index_file_writer->begin_close());
        static_cast<void>(index_file_writer->finish_close());
    }
    config::index_finish_parallelism = old_parallelism;
    state.counters["indexes"] = static_cast<double>(kinds.size());
    state.counters["finish_wall_ms"] =
            static_cast<double>(finish_wall_ns) / 1e6 / static_cast<double>(state.iterations());
    char digest[16];
    snprintf(digest, sizeof(digest), "%08x",
             file_digest(InvertedIndexDescriptor::get_index_file_path_v2(prefix)));
    state.SetLabel(digest);
}

BENCHMARK(IndexBuildSegment)
        ->ArgsProduct({{0, 1, 4, 8, 16}, {50000}})
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();

} // namespace doris::inverted_index_build_bench
