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

#include <aws/core/http/HttpResponse.h>
#include <bvar/reducer.h>
#include <gtest/gtest.h>

#include <deque>
#include <memory>
#include <string>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/obj_storage_client.h"
#include "io/fs/s3_file_reader.h"
#include "io/fs/s3_file_system.h"
#include "util/s3_util.h"
#include "util/slice.h"

namespace doris::io {

extern bvar::Adder<uint64_t> s3_file_reader_server_error_retry_counter;
extern bvar::Adder<uint64_t> s3_file_reader_transport_error_retry_counter;

namespace {

// One scripted GetObject attempt: an HTTP status and whether the body arrives.
struct Attempt {
    int http_code = 200;
    bool ok = true;
};

// Serves ranges of `data`, failing the attempts in `script` in order before succeeding.
class ScriptedObjStorageClient : public ObjStorageClient {
public:
    explicit ScriptedObjStorageClient(std::string data) : _data(std::move(data)) {}

    ObjectStorageResponse get_object(const ObjectStoragePathOptions& opts, void* buffer,
                                     size_t offset, size_t bytes_read,
                                     size_t* size_return) override {
        ++get_calls;
        Attempt attempt;
        if (!script.empty()) {
            attempt = script.front();
            script.pop_front();
        }
        if (!attempt.ok) {
            // A failed attempt may leave part of a body in the buffer.
            std::fill_n(static_cast<char*>(buffer), bytes_read / 2, 'x');
            *size_return = bytes_read / 2;
            ObjectStorageResponse resp;
            resp.status.code = ErrorCode::INTERNAL_ERROR;
            resp.status.msg = "scripted failure";
            resp.http_code = attempt.http_code;
            return resp;
        }
        memcpy(buffer, _data.data() + offset, bytes_read);
        *size_return = bytes_read;
        return ObjectStorageResponse::OK();
    }

    ObjectStorageUploadResponse create_multipart_upload(const ObjectStoragePathOptions&) override {
        return {};
    }
    ObjectStorageResponse put_object(const ObjectStoragePathOptions&, std::string_view) override {
        return ObjectStorageResponse::OK();
    }
    ObjectStorageUploadResponse upload_part(const ObjectStoragePathOptions&, std::string_view,
                                            int) override {
        return {};
    }
    ObjectStorageResponse complete_multipart_upload(
            const ObjectStoragePathOptions&, const std::vector<ObjectCompleteMultiPart>&) override {
        return ObjectStorageResponse::OK();
    }
    ObjectStorageHeadResponse head_object(const ObjectStoragePathOptions&) override { return {}; }
    ObjectStorageResponse list_objects(const ObjectStoragePathOptions&,
                                       std::vector<FileInfo>*) override {
        return ObjectStorageResponse::OK();
    }
    ObjectStorageResponse delete_objects(const ObjectStoragePathOptions&,
                                         std::vector<std::string>) override {
        return ObjectStorageResponse::OK();
    }
    ObjectStorageResponse delete_object(const ObjectStoragePathOptions&) override {
        return ObjectStorageResponse::OK();
    }
    ObjectStorageResponse delete_objects_recursively(const ObjectStoragePathOptions&) override {
        return ObjectStorageResponse::OK();
    }
    std::string generate_presigned_url(const ObjectStoragePathOptions&, int64_t,
                                       const S3ClientConf&) override {
        return "";
    }

    std::deque<Attempt> script;
    int get_calls = 0;

private:
    std::string _data;
};

class S3FileReaderRetryTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_base_wait = config::s3_read_base_wait_time_ms;
        _saved_max_wait = config::s3_read_max_wait_time_ms;
        _saved_max_retry = config::max_s3_client_retry;
        config::s3_read_base_wait_time_ms = 1;
        config::s3_read_max_wait_time_ms = 2;
        config::max_s3_client_retry = 10;
        for (int i = 0; i < 4096; ++i) {
            _data.push_back(static_cast<char>('a' + i % 26));
        }
        _client = std::make_shared<ScriptedObjStorageClient>(_data);
        auto client = _client;
        S3ClientFactory::instance().set_client_creator_for_test(
                [client](const S3ClientConf&) { return client; });
        S3ClientConf conf;
        conf.endpoint = "http://scripted";
        conf.region = "us-east-1";
        conf.ak = "ak";
        conf.sk = "sk";
        conf.bucket = "bucket";
        _holder = std::make_shared<ObjClientHolder>(conf);
        ASSERT_TRUE(_holder->init().ok());
    }

    void TearDown() override {
        S3ClientFactory::instance().clear_client_creator_for_test();
        config::s3_read_base_wait_time_ms = _saved_base_wait;
        config::s3_read_max_wait_time_ms = _saved_max_wait;
        config::max_s3_client_retry = _saved_max_retry;
    }

    Status read(size_t offset, size_t size, std::string* out) {
        S3FileReader reader(_holder, "bucket", "key", _data.size(), nullptr);
        out->assign(size, '\0');
        size_t bytes_read = 0;
        auto st = reader.read_at(offset, Slice(out->data(), size), &bytes_read, nullptr);
        out->resize(bytes_read);
        return st;
    }

    std::string _data;
    std::shared_ptr<ScriptedObjStorageClient> _client;
    std::shared_ptr<ObjClientHolder> _holder;
    int32_t _saved_base_wait = 0;
    int32_t _saved_max_wait = 0;
    int32_t _saved_max_retry = 0;
};

constexpr int kSlowDown = static_cast<int>(Aws::Http::HttpResponseCode::SERVICE_UNAVAILABLE);
constexpr int kForbidden = static_cast<int>(Aws::Http::HttpResponseCode::FORBIDDEN);
constexpr int kNoResponse = static_cast<int>(Aws::Http::HttpResponseCode::REQUEST_NOT_MADE);

} // namespace

TEST_F(S3FileReaderRetryTest, RetriesSlowDownAndBodyStreamErrorThenReturnsTheRange) {
    const uint64_t server_before = s3_file_reader_server_error_retry_counter.get_value();
    const uint64_t transport_before = s3_file_reader_transport_error_retry_counter.get_value();
    // A 503 SlowDown, then a body that broke off while streaming, then success.
    _client->script = {{kSlowDown, false}, {kNoResponse, false}};
    std::string out;
    ASSERT_TRUE(read(1000, 512, &out).ok());
    EXPECT_EQ(out, _data.substr(1000, 512));
    EXPECT_EQ(_client->get_calls, 3);
    EXPECT_EQ(s3_file_reader_server_error_retry_counter.get_value() - server_before, 1);
    EXPECT_EQ(s3_file_reader_transport_error_retry_counter.get_value() - transport_before, 1);
}

TEST_F(S3FileReaderRetryTest, RetriesServerErrorsAndShortBodies) {
    // 500, 502, 504, and a 2xx response whose body came back shorter than the range.
    _client->script = {{500, false}, {502, false}, {504, false}, {200, false}};
    std::string out;
    ASSERT_TRUE(read(0, 4096, &out).ok());
    EXPECT_EQ(out, _data);
    EXPECT_EQ(_client->get_calls, 5);
}

TEST_F(S3FileReaderRetryTest, DoesNotRetryForbidden) {
    _client->script = {{kForbidden, false}};
    std::string out;
    auto st = read(0, 128, &out);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(_client->get_calls, 1);
}

TEST_F(S3FileReaderRetryTest, StopsAfterTheRetryBudget) {
    config::max_s3_client_retry = 3;
    for (int i = 0; i < 10; ++i) {
        _client->script.push_back({kSlowDown, false});
    }
    std::string out;
    auto st = read(0, 128, &out);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(_client->get_calls, 4);
}

} // namespace doris::io
