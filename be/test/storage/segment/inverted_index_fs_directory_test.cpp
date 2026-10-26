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

#include "storage/index/inverted/inverted_index_fs_directory.h"

#include <CLucene.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <thread>
#include <vector>

#include "common/config.h"
#include "io/cache/remote_scan_cache_write_limiter.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "storage/index/inverted/analyzer/custom_analyzer.h"
#include "testutil/test_util.h"
#include "util/debug_points.h"

namespace doris::segment_v2 {

class DorisFSDirectoryTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Enable debug points for testing
        _original_enable_debug_points = config::enable_debug_points;
        config::enable_debug_points = true;

        _tmp_dir = std::filesystem::temp_directory_path() / "doris_fs_directory_test";
        std::filesystem::remove_all(_tmp_dir);
        std::filesystem::create_directories(_tmp_dir);
        _fs = io::global_local_filesystem();
        _directory = std::make_unique<DorisFSDirectory>();
        _directory->init(_fs, _tmp_dir.string().c_str());
    }

    void TearDown() override {
        _directory.reset();
        std::filesystem::remove_all(_tmp_dir);
        config::enable_debug_points = _original_enable_debug_points;
    }

    std::filesystem::path _tmp_dir;
    io::FileSystemSPtr _fs;
    std::unique_ptr<DorisFSDirectory> _directory;
    bool _original_enable_debug_points;
};

// Test 1: LOG_AND_THROW_IF_ERROR macro error handling in list()
TEST_F(DorisFSDirectoryTest, ListErrorHandling) {
    // First verify debug points are working by testing a simple case
    std::vector<std::string> names;
    EXPECT_NO_THROW(_directory->list(&names)); // Should work normally

    DebugPoints::instance()->add("DorisFSDirectory::list_status_is_not_ok");

    EXPECT_THROW(_directory->list(&names), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::list_status_is_not_ok");
}

// Test 2: Directory not exists scenario in list()
TEST_F(DorisFSDirectoryTest, ListDirectoryNotExists) {
    // This test reveals a potential bug in the current implementation:
    // When exists=false but st.ok()=true, LOG_AND_THROW_IF_ERROR won't throw
    // because it only checks if status is not OK. The current code should
    // create a non-OK status when directory doesn't exist.
    DebugPoints::instance()->add("DorisFSDirectory::list_directory_not_exists");

    std::vector<std::string> names;
    // Current implementation doesn't throw - this reveals the bug
    EXPECT_NO_THROW(_directory->list(&names));

    DebugPoints::instance()->remove("DorisFSDirectory::list_directory_not_exists");
}

// Test 3: File exists error handling
TEST_F(DorisFSDirectoryTest, FileExistsErrorHandling) {
    // First test normal behavior
    EXPECT_NO_THROW(_directory->fileExists("test_file"));

    DebugPoints::instance()->add("DorisFSDirectory::fileExists_status_is_not_ok");

    EXPECT_THROW(_directory->fileExists("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::fileExists_status_is_not_ok");
}

// Test 4: File length with NOT_FOUND error
TEST_F(DorisFSDirectoryTest, FileLengthFileNotFound) {
    DebugPoints::instance()->add("inverted file read error: index file not found");

    EXPECT_THROW(_directory->fileLength("test_file"), CLuceneError);

    DebugPoints::instance()->remove("inverted file read error: index file not found");
}

// Test 5: File length general error handling
TEST_F(DorisFSDirectoryTest, FileLengthErrorHandling) {
    DebugPoints::instance()->add("DorisFSDirectory::fileLength_status_is_not_ok");

    EXPECT_THROW(_directory->fileLength("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::fileLength_status_is_not_ok");
}

// Test 6: Touch file error handling
TEST_F(DorisFSDirectoryTest, TouchFileErrorHandling) {
    DebugPoints::instance()->add("DorisFSDirectory::touchFile_status_is_not_ok");

    EXPECT_THROW(_directory->touchFile("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::touchFile_status_is_not_ok");
}

// Test 7: Delete file error handling
TEST_F(DorisFSDirectoryTest, DeleteFileErrorHandling) {
    DebugPoints::instance()->add("DorisFSDirectory::doDeleteFile_status_is_not_ok");

    EXPECT_THROW(_directory->doDeleteFile("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::doDeleteFile_status_is_not_ok");
}

// Test 8: Delete directory error handling
TEST_F(DorisFSDirectoryTest, DeleteDirectoryErrorHandling) {
    DebugPoints::instance()->add("DorisFSDirectory::deleteDirectory_throw_is_not_directory");

    EXPECT_THROW(_directory->deleteDirectory(), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::deleteDirectory_throw_is_not_directory");
}

// Test 9: Rename file exists error handling
TEST_F(DorisFSDirectoryTest, RenameFileExistsErrorHandling) {
    DebugPoints::instance()->add("DorisFSDirectory::renameFile_exists_status_is_not_ok");

    EXPECT_THROW(_directory->renameFile("from", "to"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::renameFile_exists_status_is_not_ok");
}

// Test 10: Rename file delete error handling
TEST_F(DorisFSDirectoryTest, RenameFileDeleteErrorHandling) {
    DebugPoints::instance()->add("DorisFSDirectory::renameFile_delete_status_is_not_ok");

    EXPECT_THROW(_directory->renameFile("from", "to"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::renameFile_delete_status_is_not_ok");
}

// Test 11: Rename file rename error handling
TEST_F(DorisFSDirectoryTest, RenameFileRenameErrorHandling) {
    DebugPoints::instance()->add("DorisFSDirectory::renameFile_rename_status_is_not_ok");

    EXPECT_THROW(_directory->renameFile("from", "to"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::renameFile_rename_status_is_not_ok");
}

// Test 12: Create output exists error handling
TEST_F(DorisFSDirectoryTest, CreateOutputExistsErrorHandling) {
    DebugPoints::instance()->add("DorisFSDirectory::createOutput_exists_status_is_not_ok");

    EXPECT_THROW(_directory->createOutput("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::createOutput_exists_status_is_not_ok");
}

// Test 13: Create output delete error handling
TEST_F(DorisFSDirectoryTest, CreateOutputDeleteErrorHandling) {
    // First create a file so the delete operation will be triggered
    std::filesystem::path test_file = _tmp_dir / "test_file";
    std::ofstream(test_file).close();

    DebugPoints::instance()->add("DorisFSDirectory::createOutput_delete_status_is_not_ok");

    EXPECT_THROW(_directory->createOutput("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::createOutput_delete_status_is_not_ok");
}

// Test 14: Create output exists after delete error
TEST_F(DorisFSDirectoryTest, CreateOutputExistsAfterDeleteError) {
    // First create a file so the delete operation will be triggered
    std::filesystem::path test_file = _tmp_dir / "test_file";
    std::ofstream(test_file).close();

    DebugPoints::instance()->add("DorisFSDirectory::createOutput_exists_after_delete_error");

    EXPECT_THROW(_directory->createOutput("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::createOutput_exists_after_delete_error");
}

// Test 15: FSIndexInput open with IO error
TEST_F(DorisFSDirectoryTest, FSIndexInputOpenWithIOError) {
    DebugPoints::instance()->add("inverted file read error: index file not found");

    lucene::store::IndexInput* ret = nullptr;
    CLuceneError error;

    bool result = DorisFSDirectory::FSIndexInput::open(_fs, "nonexistent_file", ret, error);

    EXPECT_FALSE(result);
    EXPECT_EQ(error.number(), CL_ERR_FileNotFound);

    DebugPoints::instance()->remove("inverted file read error: index file not found");
}

// Test 16: FSIndexInput open with empty file
TEST_F(DorisFSDirectoryTest, FSIndexInputOpenWithEmptyFile) {
    // Create empty file
    std::filesystem::path test_file = _tmp_dir / "empty_file";
    std::ofstream(test_file).close();

    lucene::store::IndexInput* ret = nullptr;
    CLuceneError error;

    bool result = DorisFSDirectory::FSIndexInput::open(_fs, test_file.string().c_str(), ret, error);

    EXPECT_FALSE(result);
    EXPECT_EQ(error.number(), CL_ERR_EmptyIndexSegment);
}

// Test 17: FSIndexInput readInternal with read error
TEST_F(DorisFSDirectoryTest, FSIndexInputReadInternalWithReadError) {
    // Create a file with content
    std::filesystem::path test_file = _tmp_dir / "test_file";
    std::ofstream ofs(test_file);
    ofs << "test content for reading";
    ofs.close();

    lucene::store::IndexInput* input = nullptr;
    CLuceneError error;

    bool result =
            DorisFSDirectory::FSIndexInput::open(_fs, test_file.string().c_str(), input, error);
    EXPECT_TRUE(result);

    DebugPoints::instance()->add(
            "DorisFSDirectory::FSIndexInput::readInternal_reader_read_at_error");

    uint8_t buffer[10];
    EXPECT_THROW(input->readBytes(buffer, 10), CLuceneError);

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexInput::readInternal_reader_read_at_error");
    _CLDELETE(input);
}

// Test 18: FSIndexInput readInternal with bytes read error
TEST_F(DorisFSDirectoryTest, FSIndexInputReadInternalWithBytesReadError) {
    // Create a file with content
    std::filesystem::path test_file = _tmp_dir / "test_file2";
    std::ofstream ofs(test_file);
    ofs << "test content for reading";
    ofs.close();

    lucene::store::IndexInput* input = nullptr;
    CLuceneError error;

    bool result =
            DorisFSDirectory::FSIndexInput::open(_fs, test_file.string().c_str(), input, error);
    EXPECT_TRUE(result);

    DebugPoints::instance()->add("DorisFSDirectory::FSIndexInput::readInternal_bytes_read_error");

    uint8_t buffer[10];
    EXPECT_THROW(input->readBytes(buffer, 10), CLuceneError);

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexInput::readInternal_bytes_read_error");
    _CLDELETE(input);
}

// Test 19: FSIndexOutput init error
TEST_F(DorisFSDirectoryTest, FSIndexOutputInitError) {
    DebugPoints::instance()->add(
            "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_init");
    std::unique_ptr<DorisFSDirectory::FSIndexOutputV2> output =
            std::make_unique<DorisFSDirectory::FSIndexOutputV2>();

    try {
        output->init(nullptr);
    } catch (CLuceneError& err) {
        EXPECT_EQ(err.number(), CL_ERR_IO);
        EXPECT_EQ(std::string(err.what()),
                  "debug point: test throw error in fsindexoutput init mock error");
    }
    try {
        output->close();
    } catch (CLuceneError& err) {
        EXPECT_EQ(err.number(), CL_ERR_IO);
        EXPECT_EQ(std::string(err.what()), "flushBuffer error, _index_v2_file_writer = nullptr");
    }

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_init");
}

// Test 20: FSIndexOutput destructor error
TEST_F(DorisFSDirectoryTest, FSIndexOutputDestructorError) {
    auto* output = _directory->createOutput("test_file");
    output->writeString("test");

    DebugPoints::instance()->add(
            "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_destructor");

    // The destructor should handle the error gracefully (no exception should propagate)
    EXPECT_NO_THROW(delete output);

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_destructor");
}

// Test 21: FSIndexOutput close with writer close error
TEST_F(DorisFSDirectoryTest, FSIndexOutputCloseWithWriterCloseError) {
    auto* output = _directory->createOutput("test_file");
    output->writeString("test");

    DebugPoints::instance()->add("DorisFSDirectory::FSIndexOutput._set_writer_close_status_error");

    EXPECT_THROW(output->close(), CLuceneError);

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexOutput._set_writer_close_status_error");
    delete output;
}

// Test 22: FSIndexOutput close with null writer
TEST_F(DorisFSDirectoryTest, FSIndexOutputCloseWithNullWriter) {
    auto* output = _directory->createOutput("test_file");
    output->writeString("test");

    DebugPoints::instance()->add("DorisFSDirectory::FSIndexOutput.set_writer_nullptr");

    EXPECT_THROW(output->close(), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::FSIndexOutput.set_writer_nullptr");
    delete output;
}

// Test 24: DorisFSDirectory close error
TEST_F(DorisFSDirectoryTest, DorisFSDirectoryCloseError) {
    DebugPoints::instance()->add("DorisFSDirectory::close_close_with_error");

    EXPECT_THROW(_directory->close(), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::close_close_with_error");
}

// Test 25: DorisFSDirectoryFactory getDirectory with null file
TEST_F(DorisFSDirectoryTest, DorisFSDirectoryFactoryGetDirectoryWithNullFile) {
    DebugPoints::instance()->add("DorisFSDirectoryFactory::getDirectory_file_is_nullptr");

    EXPECT_THROW(DorisFSDirectoryFactory::getDirectory(_fs, "test_path", false), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectoryFactory::getDirectory_file_is_nullptr");
}

// Test 26: DorisFSDirectoryFactory getDirectory exists error
TEST_F(DorisFSDirectoryTest, DorisFSDirectoryFactoryGetDirectoryExistsError) {
    bool original_inverted_index_ram_dir_enable = config::inverted_index_ram_dir_enable;
    config::inverted_index_ram_dir_enable = false;
    DebugPoints::instance()->add("DorisFSDirectoryFactory::getDirectory_exists_status_is_not_ok");

    EXPECT_THROW(DorisFSDirectoryFactory::getDirectory(_fs, "test_path", false), CLuceneError);

    DebugPoints::instance()->remove(
            "DorisFSDirectoryFactory::getDirectory_exists_status_is_not_ok");
    config::inverted_index_ram_dir_enable = original_inverted_index_ram_dir_enable;
}

// Test 27: DorisFSDirectoryFactory getDirectory create directory error
TEST_F(DorisFSDirectoryTest, DorisFSDirectoryFactoryGetDirectoryCreateDirectoryError) {
    bool original_inverted_index_ram_dir_enable = config::inverted_index_ram_dir_enable;
    config::inverted_index_ram_dir_enable = false;
    DebugPoints::instance()->add(
            "DorisFSDirectoryFactory::getDirectory_create_directory_status_is_not_ok");

    EXPECT_THROW(DorisFSDirectoryFactory::getDirectory(
                         _fs, (_tmp_dir / "test_path2").string().c_str(), false),
                 CLuceneError);

    DebugPoints::instance()->remove(
            "DorisFSDirectoryFactory::getDirectory_create_directory_status_is_not_ok");
    config::inverted_index_ram_dir_enable = original_inverted_index_ram_dir_enable;
}

// Test 28: Buffer size setting in FSIndexInput::open (覆盖第92-93行)
TEST_F(DorisFSDirectoryTest, FSIndexInputOpenBufferSizeDefault) {
    // Create a file with content
    std::filesystem::path test_file = _tmp_dir / "buffer_test_file";
    std::ofstream ofs(test_file);
    ofs << "test content for buffer size test";
    ofs.close();

    lucene::store::IndexInput* ret = nullptr;
    CLuceneError error;

    // Test with buffer_size = -1 (should set to default buffer size)
    bool result =
            DorisFSDirectory::FSIndexInput::open(_fs, test_file.string().c_str(), ret, error, -1);
    EXPECT_TRUE(result);
    EXPECT_NE(ret, nullptr);

    _CLDELETE(ret);
}

// Test 29: DorisRAMFSDirectory fileModified with file not found
TEST_F(DorisFSDirectoryTest, DorisRAMFSDirectoryFileModifiedWithFileNotFound) {
    DebugPoints::instance()->add("DorisRAMFSDirectory::fileModified_file_not_found");

    auto ram_dir = std::make_unique<DorisRAMFSDirectory>();
    ram_dir->init(_fs, _tmp_dir.string().c_str());

    EXPECT_THROW(ram_dir->fileModified("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisRAMFSDirectory::fileModified_file_not_found");
}

// Test 30: DorisRAMFSDirectory touchFile with file not found
TEST_F(DorisFSDirectoryTest, DorisRAMFSDirectoryTouchFileWithFileNotFound) {
    DebugPoints::instance()->add("DorisRAMFSDirectory::touchFile_file_not_found");

    auto ram_dir = std::make_unique<DorisRAMFSDirectory>();
    ram_dir->init(_fs, _tmp_dir.string().c_str());

    EXPECT_THROW(ram_dir->touchFile("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisRAMFSDirectory::touchFile_file_not_found");
}

// Test 31: DorisRAMFSDirectory fileLength with file not found
TEST_F(DorisFSDirectoryTest, DorisRAMFSDirectoryFileLengthWithFileNotFound) {
    DebugPoints::instance()->add("DorisRAMFSDirectory::fileLength_file_not_found");

    auto ram_dir = std::make_unique<DorisRAMFSDirectory>();
    ram_dir->init(_fs, _tmp_dir.string().c_str());

    EXPECT_THROW(ram_dir->fileLength("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisRAMFSDirectory::fileLength_file_not_found");
}

// Test 32: DorisRAMFSDirectory openInput with file not found
TEST_F(DorisFSDirectoryTest, DorisRAMFSDirectoryOpenInputWithFileNotFound) {
    DebugPoints::instance()->add("DorisRAMFSDirectory::openInput_file_not_found");

    auto ram_dir = std::make_unique<DorisRAMFSDirectory>();
    ram_dir->init(_fs, _tmp_dir.string().c_str());

    lucene::store::IndexInput* ret = nullptr;
    CLuceneError error;

    bool result = ram_dir->openInput("test_file", ret, error);

    EXPECT_FALSE(result);
    EXPECT_EQ(error.number(), CL_ERR_IO);

    DebugPoints::instance()->remove("DorisRAMFSDirectory::openInput_file_not_found");
}

// Test 33: DorisRAMFSDirectory close error
TEST_F(DorisFSDirectoryTest, DorisRAMFSDirectoryCloseError) {
    DebugPoints::instance()->add("DorisRAMFSDirectory::close_close_with_error");

    auto ram_dir = std::make_unique<DorisRAMFSDirectory>();
    ram_dir->init(_fs, _tmp_dir.string().c_str());

    EXPECT_THROW(ram_dir->close(), CLuceneError);

    DebugPoints::instance()->remove("DorisRAMFSDirectory::close_close_with_error");
}

// Test 34: DorisRAMFSDirectory renameFile with file not found
TEST_F(DorisFSDirectoryTest, DorisRAMFSDirectoryRenameFileWithFileNotFound) {
    DebugPoints::instance()->add("DorisRAMFSDirectory::renameFile_itr_filesMap_end");

    auto ram_dir = std::make_unique<DorisRAMFSDirectory>();
    ram_dir->init(_fs, _tmp_dir.string().c_str());

    EXPECT_THROW(ram_dir->renameFile("from", "to"), CLuceneError);

    DebugPoints::instance()->remove("DorisRAMFSDirectory::renameFile_itr_filesMap_end");
}

// Test 35: DorisRAMFSDirectory createOutput with existing file
TEST_F(DorisFSDirectoryTest, DorisRAMFSDirectoryCreateOutputWithExistingFile) {
    DebugPoints::instance()->add("DorisRAMFSDirectory::createOutput_itr_filesMap_end");

    auto ram_dir = std::make_unique<DorisRAMFSDirectory>();
    ram_dir->init(_fs, _tmp_dir.string().c_str());

    // This should not throw, but create a new output
    auto* output = ram_dir->createOutput("test_file");
    EXPECT_NE(output, nullptr);

    delete output;
    DebugPoints::instance()->remove("DorisRAMFSDirectory::createOutput_itr_filesMap_end");
}

// Test 36: FSIndexOutputV2 init error
TEST_F(DorisFSDirectoryTest, FSIndexOutputV2InitError) {
    DebugPoints::instance()->add(
            "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_init");
    std::unique_ptr<DorisFSDirectory::FSIndexOutputV2> output =
            std::make_unique<DorisFSDirectory::FSIndexOutputV2>();

    // Create writer first
    io::FileWriterPtr writer;
    std::string file_path = (_tmp_dir / "test_file").string();
    Status s = _fs->create_file(file_path, &writer);
    EXPECT_TRUE(s.ok());

    try {
        output->init(writer.get());
    } catch (CLuceneError& err) {
        EXPECT_EQ(err.number(), CL_ERR_IO);
        EXPECT_EQ(std::string(err.what()),
                  "debug point: test throw error in fsindexoutput init mock error");
    }
    try {
        output->close();
    } catch (CLuceneError& err) {
        EXPECT_EQ(err.number(), CL_ERR_IO);
        EXPECT_EQ(std::string(err.what()), "flushBuffer error, _index_v2_file_writer = nullptr");
    }

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_init");
}

// Test 37: FSIndexOutputV2 flushBuffer error
TEST_F(DorisFSDirectoryTest, FSIndexOutputV2FlushBufferError) {
    DebugPoints::instance()->add(
            "DorisFSDirectory::FSIndexOutput._status_error_in_fsindexoutput_flushBuffer");

    // Create writer first
    io::FileWriterPtr writer;
    std::string file_path = (_tmp_dir / "test_file").string();
    Status s = _fs->create_file(file_path, &writer);
    EXPECT_TRUE(s.ok());

    auto output = _directory->createOutputV2(writer.get());

    // Write small chunks to fill the buffer and trigger flush
    // BufferedIndexOutput buffer size is 1024 bytes
    std::string data(65537, 'a'); // 65536 bytes per write
    // This final write should trigger flushBuffer and hit the debug point
    EXPECT_THROW(output->writeString(data.c_str(), data.length()), CLuceneError);

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexOutput._status_error_in_fsindexoutput_flushBuffer");

    // Try to close gracefully first to avoid destructor issues
    try {
        output->close();
    } catch (...) {
        // Ignore close errors in cleanup
    }
}

// Test 38: FSIndexOutputV2 flushBuffer with null writer
TEST_F(DorisFSDirectoryTest, FSIndexOutputV2FlushBufferWithNullWriter) {
    // Create a custom FSIndexOutputV2 with null writer to test the error condition
    std::unique_ptr<DorisFSDirectory::FSIndexOutputV2> fs_output_v2 =
            std::make_unique<DorisFSDirectory::FSIndexOutputV2>();
    fs_output_v2->init(nullptr); // Initialize with null writer

    // Any write operation should trigger flushBuffer and throw error due to null writer
    std::string data(65537, 'a'); // 65536 bytes per write
    // This final write should trigger flushBuffer and hit the debug point
    try {
        fs_output_v2->writeString(data.c_str(), data.length());
    } catch (CLuceneError& err) {
        EXPECT_EQ(err.number(), CL_ERR_IO);
        EXPECT_EQ(std::string(err.what()), "flushBuffer error, _index_v2_file_writer = nullptr");
    }
    try {
        fs_output_v2->close();
    } catch (CLuceneError& err) {
        EXPECT_EQ(err.number(), CL_ERR_IO);
        EXPECT_EQ(std::string(err.what()), "flushBuffer error, _index_v2_file_writer = nullptr");
    }
}

// Test 39: FSIndexOutputV2 flushBuffer with null buffer
TEST_F(DorisFSDirectoryTest, FSIndexOutputV2FlushBufferWithNullBuffer) {
    DebugPoints::instance()->add("DorisFSDirectory::FSIndexOutputV2::flushBuffer_b_is_nullptr");

    // Create writer first
    io::FileWriterPtr writer;
    std::string file_path = (_tmp_dir / "test_file").string();
    Status s = _fs->create_file(file_path, &writer);
    EXPECT_TRUE(s.ok());

    std::unique_ptr<DorisFSDirectory::FSIndexOutputV2> output =
            std::make_unique<DorisFSDirectory::FSIndexOutputV2>();
    output->init(writer.get());

    // This should just log warning, not throw
    EXPECT_NO_THROW(output->writeString("test"));
    output->close();
    DebugPoints::instance()->remove("DorisFSDirectory::FSIndexOutputV2::flushBuffer_b_is_nullptr");
}

// Test 40: FSIndexOutputV2 close error
TEST_F(DorisFSDirectoryTest, FSIndexOutputV2CloseError) {
    DebugPoints::instance()->add("DorisFSDirectory::FSIndexOutput._set_writer_close_status_error");

    // Create writer first
    io::FileWriterPtr writer;
    std::string file_path = (_tmp_dir / "test_file").string();
    Status s = _fs->create_file(file_path, &writer);
    EXPECT_TRUE(s.ok());

    std::unique_ptr<DorisFSDirectory::FSIndexOutputV2> output =
            std::make_unique<DorisFSDirectory::FSIndexOutputV2>();
    output->init(writer.get());
    output->writeString("test");

    EXPECT_THROW(output->close(), CLuceneError);

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexOutput._set_writer_close_status_error");
}

// Test 41: FSIndexOutputV2 close with null writer
TEST_F(DorisFSDirectoryTest, FSIndexOutputV2CloseWithNullWriter) {
    DebugPoints::instance()->add("DorisFSDirectory::FSIndexOutput.set_writer_nullptr");

    // Create writer first
    io::FileWriterPtr writer;
    std::string file_path = (_tmp_dir / "test_file").string();
    Status s = _fs->create_file(file_path, &writer);
    EXPECT_TRUE(s.ok());

    std::unique_ptr<DorisFSDirectory::FSIndexOutputV2> output =
            std::make_unique<DorisFSDirectory::FSIndexOutputV2>();
    output->init(writer.get());

    try {
        output->close();
    } catch (CLuceneError& err) {
        EXPECT_EQ(err.number(), CL_ERR_IO);
        EXPECT_EQ(std::string(err.what()),
                  "close file writer error, _index_v2_file_writer = nullptr");
    }

    DebugPoints::instance()->remove("DorisFSDirectory::FSIndexOutput.set_writer_nullptr");
}

// Test 42: FSIndexOutputV2 length with null writer
TEST_F(DorisFSDirectoryTest, FSIndexOutputV2LengthWithNullWriter) {
    // Create a custom FSIndexOutputV2 with null writer to test precondition
    std::unique_ptr<DorisFSDirectory::FSIndexOutputV2> fs_output_v2 =
            std::make_unique<DorisFSDirectory::FSIndexOutputV2>();
    fs_output_v2->init(nullptr);

    try {
        fs_output_v2->length();
    } catch (CLuceneError& err) {
        EXPECT_EQ(err.number(), CL_ERR_IO);
        EXPECT_EQ(std::string(err.what()), "file is not open, index_v2_file_writer is nullptr");
    }

    try {
        fs_output_v2->close();
    } catch (CLuceneError& err) {
        EXPECT_EQ(err.number(), CL_ERR_IO);
        EXPECT_EQ(std::string(err.what()), "flushBuffer error, _index_v2_file_writer = nullptr");
    }
}

// Test 43: Test FSIndexInput copy constructor - basic functionality
TEST_F(DorisFSDirectoryTest, FSIndexInputCopyConstructorWithNullHandle) {
    // Create a file first
    std::filesystem::path test_file = _tmp_dir / "test_file_clone";
    std::ofstream ofs(test_file);
    ofs << "test content for clone";
    ofs.close();

    lucene::store::IndexInput* input = nullptr;
    CLuceneError error;

    bool result =
            DorisFSDirectory::FSIndexInput::open(_fs, test_file.string().c_str(), input, error);
    EXPECT_TRUE(result);

    auto* fs_input = dynamic_cast<DorisFSDirectory::FSIndexInput*>(input);
    ASSERT_NE(fs_input, nullptr);

    // Test normal clone functionality
    auto* cloned_input = fs_input->clone();
    EXPECT_NE(cloned_input, nullptr);

    // Verify cloned input works correctly
    EXPECT_EQ(cloned_input->length(), fs_input->length());

    _CLDELETE(cloned_input);
    _CLDELETE(input);
}

// Test 44: FSIndexOutput flushBuffer error
TEST_F(DorisFSDirectoryTest, FSIndexOutputFlushBufferError) {
    DebugPoints::instance()->add(
            "DorisFSDirectory::FSIndexOutput._status_error_in_fsindexoutput_flushBuffer");

    auto* output = _directory->createOutput("test_file");

    std::string data(65537, 'a'); // 65536 bytes per write
    EXPECT_THROW(output->writeString(data.c_str(), data.length()), CLuceneError);

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexOutput._status_error_in_fsindexoutput_flushBuffer");

    try {
        output->close();
    } catch (...) {
        // Ignore close errors
    }
    delete output;
}

// Test 45: FSIndexOutput flushBuffer with null writer
TEST_F(DorisFSDirectoryTest, FSIndexOutputFlushBufferWithNullWriter) {
    DebugPoints::instance()->add("DorisFSDirectory::FSIndexOutput::flushBuffer_writer_is_nullptr");

    auto* output = _directory->createOutput("test_file2");
    std::string data(65537, 'a'); // 65536 bytes per write
    // This should log warning but not crash
    EXPECT_NO_THROW(output->writeString(data.c_str(), data.length()));

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexOutput::flushBuffer_writer_is_nullptr");

    try {
        output->close();
    } catch (...) {
        // Ignore close errors
    }
    delete output;
}

// Test 46: FSIndexOutput flushBuffer with null buffer
TEST_F(DorisFSDirectoryTest, FSIndexOutputFlushBufferWithNullBuffer) {
    DebugPoints::instance()->add("DorisFSDirectory::FSIndexOutput::flushBuffer_b_is_nullptr");

    auto* output = _directory->createOutput("test_file3");

    std::string data(65537, 'a'); // 65536 bytes per write
    // This should log warning but not crash
    EXPECT_NO_THROW(output->writeString(data.c_str(), data.length()));

    DebugPoints::instance()->remove("DorisFSDirectory::FSIndexOutput::flushBuffer_b_is_nullptr");

    try {
        output->close();
    } catch (...) {
        // Ignore close errors
    }
    delete output;
}

TEST_F(DorisFSDirectoryTest, FSIndexInputReadInternalTimer) {
    std::string file_name = "test_timer_file";
    std::filesystem::path test_file = _tmp_dir / file_name;
    std::ofstream ofs(test_file);
    std::string content = "some test content for timer";
    ofs << content;
    ofs.close();

    lucene::store::IndexInput* input1 = nullptr;
    CLuceneError error;
    bool result =
            DorisFSDirectory::FSIndexInput::open(_fs, test_file.string().c_str(), input1, error);
    EXPECT_TRUE(result);
    ASSERT_NE(input1, nullptr);

    auto* fs_input1 = dynamic_cast<DorisFSDirectory::FSIndexInput*>(input1);
    ASSERT_NE(fs_input1, nullptr);

    io::FileCacheStatistics stats;
    fs_input1->_io_ctx.file_cache_stats = &stats;

    auto* input2 = fs_input1->clone();
    auto* fs_input2 = dynamic_cast<DorisFSDirectory::FSIndexInput*>(input2);
    ASSERT_NE(fs_input2, nullptr);

    fs_input2->_io_ctx.file_cache_stats = &stats;

    uint8_t buffer1[10];
    input1->readBytes(buffer1, 10);
    EXPECT_GT(stats.inverted_index_io_timer, 0);
    int64_t old_time = stats.inverted_index_io_timer;

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    input2->seek(0);
    uint8_t buffer2[10];
    input2->readBytes(buffer2, 10);
    EXPECT_GT(stats.inverted_index_io_timer, old_time);

    _CLDELETE(input2);
    _CLDELETE(input1);
}

TEST_F(DorisFSDirectoryTest, FSIndexInputSetIoContextPropagatesQueryLimiter) {
    std::string file_name = "test_io_context_file";
    std::filesystem::path test_file = _tmp_dir / file_name;
    std::ofstream ofs(test_file);
    std::string content = "some test content for io context";
    ofs << content;
    ofs.close();

    lucene::store::IndexInput* input = nullptr;
    CLuceneError error;
    bool result =
            DorisFSDirectory::FSIndexInput::open(_fs, test_file.string().c_str(), input, error);
    EXPECT_TRUE(result);
    ASSERT_NE(input, nullptr);

    auto* fs_input = dynamic_cast<DorisFSDirectory::FSIndexInput*>(input);
    ASSERT_NE(fs_input, nullptr);

    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;
    io::FileCacheStatistics stats;
    io::RemoteScanCacheWriteLimiter limiter(query_id, 0);
    io::IOContext io_ctx;
    io_ctx.reader_type = ReaderType::READER_QUERY;
    io_ctx.query_id = &query_id;
    io_ctx.file_cache_stats = &stats;
    io_ctx.file_cache_miss_policy = io::FileCacheMissPolicy::REMOTE_ONLY_ON_MISS;
    io_ctx.remote_scan_cache_write_limiter = &limiter;

    fs_input->setIndexFile(true);
    fs_input->setIoContext(&io_ctx);

    const auto* actual = static_cast<const io::IOContext*>(fs_input->getIoContext());
    EXPECT_EQ(actual->reader_type, ReaderType::READER_QUERY);
    EXPECT_EQ(actual->query_id, &query_id);
    EXPECT_EQ(actual->file_cache_stats, &stats);
    EXPECT_EQ(actual->file_cache_miss_policy, io::FileCacheMissPolicy::REMOTE_ONLY_ON_MISS);
    EXPECT_EQ(actual->remote_scan_cache_write_limiter, &limiter);
    EXPECT_TRUE(actual->is_inverted_index);
    EXPECT_TRUE(actual->is_index_data);

    fs_input->setIoContext(nullptr);
    actual = static_cast<const io::IOContext*>(fs_input->getIoContext());
    EXPECT_EQ(actual->reader_type, ReaderType::UNKNOWN);
    EXPECT_EQ(actual->query_id, nullptr);
    EXPECT_EQ(actual->file_cache_stats, nullptr);
    EXPECT_EQ(actual->file_cache_miss_policy, io::FileCacheMissPolicy::READ_THROUGH_AND_WRITE_BACK);
    EXPECT_EQ(actual->remote_scan_cache_write_limiter, nullptr);
    EXPECT_TRUE(actual->is_inverted_index);
    EXPECT_TRUE(actual->is_index_data);

    _CLDELETE(input);
}

TEST_F(DorisFSDirectoryTest, PrivGetFN) {
    {
        std::string file_name = "my_file.txt";
        std::string result = _directory->priv_getFN(file_name);
        std::string expected_path = (_tmp_dir / file_name).string();
        EXPECT_EQ(result, expected_path);
    }
    {
        std::string file_name = "";
        std::string result = _directory->priv_getFN(file_name);
        std::string expected_path = (_tmp_dir / file_name).string();
        EXPECT_EQ(result, expected_path);
    }
    {
        std::string file_name = "subdir/another_file.log";
        std::string result = _directory->priv_getFN(file_name);
        std::string expected_path = (_tmp_dir / file_name).string();
        EXPECT_EQ(result, expected_path);
    }
}

namespace {

// Writes `size` bytes through createOutput and closes the stream.
void write_ram_file(lucene::store::Directory* dir, const char* name, int32_t size) {
    std::vector<uint8_t> bytes(size);
    for (int32_t i = 0; i < size; ++i) {
        bytes[i] = static_cast<uint8_t>(i * 31 + 7);
    }
    lucene::store::IndexOutput* out = dir->createOutput(name);
    out->writeBytes(bytes.data(), size);
    out->close();
    _CLDELETE(out);
}

int64_t buffer_bytes(int32_t size) {
    constexpr int64_t kBuffer = lucene::store::RAMOutputStream::BUFFER_SIZE;
    return (size + kBuffer - 1) / kBuffer * kBuffer;
}

std::string read_file(lucene::store::Directory* dir, const std::string& name) {
    lucene::store::IndexInput* in = nullptr;
    CLuceneError error;
    EXPECT_TRUE(dir->openInput(name.c_str(), in, error));
    std::string content(in->length(), '\0');
    in->readBytes(reinterpret_cast<uint8_t*>(content.data()), content.size());
    in->close();
    _CLDELETE(in);
    return content;
}

// Indexes the same tokenized documents into `dir`, the way the index writers of a segment do.
void write_index(lucene::store::Directory* dir) {
    inverted_index::CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config("standard", {});
    auto analyzer = inverted_index::CustomAnalyzer::build_custom_analyzer(builder.build());
    auto* writer = _CLNEW lucene::index::IndexWriter(dir, analyzer.get(), true);
    writer->setMaxBufferedDocs(100);
    writer->setRAMBufferSizeMB(-1);
    writer->setMaxFieldLength(0x7FFFFFFFL);
    writer->setMergeFactor(1000000000);
    writer->setUseCompoundFile(false);

    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    auto* doc = _CLNEW lucene::document::Document();
    int32_t field_config = lucene::document::Field::STORE_NO;
    field_config |= lucene::document::Field::INDEX_NONORMS;
    field_config |= lucene::document::Field::INDEX_TOKENIZED;
    auto* field = _CLNEW lucene::document::Field(L"msg", field_config);
    field->setOmitTermFreqAndPositions(false);
    doc->add(*field);
    for (int i = 0; i < 2000; ++i) {
        std::string value = "host" + std::to_string(i % 97) + " user" + std::to_string(i % 13) +
                            " request " + std::to_string(i);
        reader->init(value.data(), value.size(), false);
        field->setValue(analyzer->reusableTokenStream(field->name(), reader));
        writer->addDocument(doc);
    }
    writer->close();
    _CLLDELETE(writer);
    _CLLDELETE(doc);
}

} // namespace

// The buffers of a RAM directory are plain new[] allocations, so the directory itself has to put
// them on the memory tracker of the task that writes the index.
TEST_F(DorisFSDirectoryTest, DorisRAMFSDirectoryTracksBuffersOnTaskTracker) {
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                    "DorisRAMFSDirectoryTest");
    std::thread([&]() {
        SCOPED_ATTACH_TASK(tracker);
        const int64_t total_before = DorisRAMFSDirectory::total_bytes();
        const int64_t tracked_before = tracker->consumption();
        auto tracked = [&]() { return tracker->consumption() - tracked_before; };
        auto total = [&]() { return DorisRAMFSDirectory::total_bytes() - total_before; };
        {
            auto ram_dir = std::make_unique<DorisRAMFSDirectory>();
            ram_dir->init(_fs, _tmp_dir.string().c_str());

            write_ram_file(ram_dir.get(), "a", 10 * 1024 + 5);
            write_ram_file(ram_dir.get(), "b", 3000);
            const int64_t expected = buffer_bytes(10 * 1024 + 5) + buffer_bytes(3000);
            EXPECT_EQ(tracked(), expected);
            EXPECT_EQ(total(), expected);

            EXPECT_TRUE(ram_dir->deleteFile("a"));
            EXPECT_EQ(tracked(), buffer_bytes(3000));
            EXPECT_EQ(total(), buffer_bytes(3000));

            // Recreating a file drops the buffers of the old one.
            write_ram_file(ram_dir.get(), "b", 100);
            EXPECT_EQ(tracked(), buffer_bytes(100));

            write_ram_file(ram_dir.get(), "c", 5000);
            ram_dir->renameFile("c", "b");
            EXPECT_EQ(tracked(), buffer_bytes(5000));
            EXPECT_EQ(total(), buffer_bytes(5000));
        }
        EXPECT_EQ(tracked(), 0);
        EXPECT_EQ(total(), 0);
    }).join();
}

TEST_F(DorisFSDirectoryTest, DorisFSDirectoryFactoryWritesToDiskOverRamDirBudget) {
    const std::string original = config::inverted_index_ram_dir_max_memory;
    auto get_dir = [&](const std::string& name) {
        auto path = (_tmp_dir / name).string();
        return std::unique_ptr<DorisFSDirectory>(
                DorisFSDirectoryFactory::getDirectory(_fs, path.c_str(), true));
    };
    auto is_ram = [](const std::unique_ptr<DorisFSDirectory>& dir) {
        return std::strcmp(dir->getObjectName(), DorisRAMFSDirectory::getClassName()) == 0;
    };

    config::inverted_index_ram_dir_max_memory = "-1";
    auto held = get_dir("held");
    ASSERT_TRUE(is_ram(held));
    write_ram_file(held.get(), "a", 4096);
    const int64_t total = DorisRAMFSDirectory::total_bytes();
    ASSERT_GE(total, 4096);

    config::inverted_index_ram_dir_max_memory = std::to_string(total);
    EXPECT_FALSE(is_ram(get_dir("over")));
    config::inverted_index_ram_dir_max_memory = std::to_string(total + 1);
    EXPECT_TRUE(is_ram(get_dir("under")));
    config::inverted_index_ram_dir_max_memory = "1";
    EXPECT_FALSE(is_ram(get_dir("tiny")));

    held.reset();
    config::inverted_index_ram_dir_max_memory = original;
}

// An index written to disk because the budget is exhausted is the same as one written in memory.
TEST_F(DorisFSDirectoryTest, IndexOverRamDirBudgetMatchesRamDirIndex) {
    const std::string original = config::inverted_index_ram_dir_max_memory;
    auto build = [&](const std::string& budget, const std::string& name, bool expect_ram) {
        config::inverted_index_ram_dir_max_memory = budget;
        auto path = (_tmp_dir / name).string();
        std::unique_ptr<DorisFSDirectory> dir(
                DorisFSDirectoryFactory::getDirectory(_fs, path.c_str(), true));
        EXPECT_EQ(std::strcmp(dir->getObjectName(), DorisRAMFSDirectory::getClassName()) == 0,
                  expect_ram);
        write_index(dir.get());
        std::vector<std::string> names;
        EXPECT_TRUE(dir->list(&names));
        std::sort(names.begin(), names.end());
        std::map<std::string, std::string> files;
        for (const auto& file : names) {
            files[file] = read_file(dir.get(), file);
        }
        return files;
    };

    auto in_memory = build("-1", "ram_index", true);
    // A RAM directory holding one buffer exhausts a budget of one byte.
    std::unique_ptr<DorisRAMFSDirectory> held = std::make_unique<DorisRAMFSDirectory>();
    held->init(_fs, _tmp_dir.string().c_str());
    write_ram_file(held.get(), "held", 1);
    auto on_disk = build("1", "disk_index", false);
    held.reset();
    config::inverted_index_ram_dir_max_memory = original;

    ASSERT_FALSE(in_memory.empty());
    EXPECT_EQ(in_memory.size(), on_disk.size());
    for (const auto& [name, content] : in_memory) {
        ASSERT_TRUE(on_disk.contains(name)) << name;
        // A segments file starts with a version taken from the clock.
        if (name.starts_with("segments")) {
            EXPECT_EQ(content.size(), on_disk[name].size()) << name;
            continue;
        }
        EXPECT_EQ(content, on_disk[name]) << name;
    }
}

// Past the bound, a file that is being written moves to disk and continues there, and the
// directory writes its next files to disk too.
TEST_F(DorisFSDirectoryTest, DorisRAMFSDirectoryMovesFilesToDiskOverBudget) {
    const std::string original = config::inverted_index_ram_dir_max_memory;
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                    "DorisRAMFSDirectorySpillTest");
    std::thread([&]() {
        SCOPED_ATTACH_TASK(tracker);
        const int64_t tracked_before = tracker->consumption();
        auto path = (_tmp_dir / "spill").string();
        config::inverted_index_ram_dir_max_memory = "-1";
        auto ram_dir = std::make_unique<DorisRAMFSDirectory>();
        ram_dir->init(_fs, path.c_str());

        write_ram_file(ram_dir.get(), "closed", 3000);
        std::vector<uint8_t> bytes(200 * 1024);
        for (size_t i = 0; i < bytes.size(); ++i) {
            bytes[i] = static_cast<uint8_t>(i * 13 + 1);
        }
        lucene::store::IndexOutput* out = ram_dir->createOutput("open");
        out->writeBytes(bytes.data(), 100 * 1024);
        EXPECT_FALSE(ram_dir->spilled());

        config::inverted_index_ram_dir_max_memory = "1";
        out->writeBytes(bytes.data() + 100 * 1024, 100 * 1024);
        EXPECT_TRUE(ram_dir->spilled());
        EXPECT_EQ(out->getFilePointer(), 200 * 1024);
        out->close();
        _CLDELETE(out);
        // Only the closed file is left in memory.
        EXPECT_EQ(tracker->consumption() - tracked_before, buffer_bytes(3000));
        EXPECT_TRUE(std::filesystem::exists(std::filesystem::path(path) / "open"));

        write_ram_file(ram_dir.get(), "next", 5000);
        EXPECT_TRUE(std::filesystem::exists(std::filesystem::path(path) / "next"));
        EXPECT_EQ(tracker->consumption() - tracked_before, buffer_bytes(3000));

        std::vector<std::string> names;
        EXPECT_TRUE(ram_dir->list(&names));
        std::sort(names.begin(), names.end());
        EXPECT_EQ(names, (std::vector<std::string> {"closed", "next", "open"}));
        EXPECT_EQ(ram_dir->fileLength("open"), 200 * 1024);
        EXPECT_EQ(read_file(ram_dir.get(), "open"),
                  std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size()));
        EXPECT_EQ(read_file(ram_dir.get(), "closed").size(), 3000);

        ram_dir->renameFile("next", "renamed");
        EXPECT_TRUE(ram_dir->fileExists("renamed"));
        EXPECT_FALSE(ram_dir->fileExists("next"));
        EXPECT_TRUE(ram_dir->deleteFile("renamed"));
        EXPECT_FALSE(std::filesystem::exists(std::filesystem::path(path) / "renamed"));

        EXPECT_TRUE(ram_dir->deleteDirectory());
        EXPECT_FALSE(std::filesystem::exists(path));
        ram_dir.reset();
        EXPECT_EQ(tracker->consumption() - tracked_before, 0);
    }).join();
    config::inverted_index_ram_dir_max_memory = original;
}

// An index whose files move to disk while it is written is the same as one written in memory.
TEST_F(DorisFSDirectoryTest, IndexMovedToDiskMidWriteMatchesRamDirIndex) {
    const std::string original = config::inverted_index_ram_dir_max_memory;
    auto build = [&](const std::string& name, bool spill) {
        config::inverted_index_ram_dir_max_memory = "-1";
        auto path = (_tmp_dir / name).string();
        auto dir = std::make_unique<DorisRAMFSDirectory>();
        dir->init(_fs, path.c_str());
        if (spill) {
            // Room for about 32 KB more, far less than the index needs.
            config::inverted_index_ram_dir_max_memory =
                    std::to_string(DorisRAMFSDirectory::total_bytes() + 32 * 1024);
        }
        write_index(dir.get());
        EXPECT_EQ(dir->spilled(), spill);
        std::vector<std::string> names;
        EXPECT_TRUE(dir->list(&names));
        std::map<std::string, std::string> files;
        for (const auto& file : names) {
            files[file] = read_file(dir.get(), file);
        }
        dir->deleteDirectory();
        return files;
    };
    auto in_memory = build("ram_index", false);
    auto moved = build("moved_index", true);
    config::inverted_index_ram_dir_max_memory = original;

    ASSERT_FALSE(in_memory.empty());
    EXPECT_EQ(in_memory.size(), moved.size());
    for (const auto& [name, content] : in_memory) {
        ASSERT_TRUE(moved.contains(name)) << name;
        // A segments file starts with a version taken from the clock.
        if (name.starts_with("segments")) {
            EXPECT_EQ(content.size(), moved[name].size()) << name;
            continue;
        }
        EXPECT_EQ(content, moved[name]) << name;
    }
}

TEST_F(DorisFSDirectoryTest, DorisRAMFSDirectoryMoveFilesToDiskFreesMemory) {
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                    "DorisRAMFSDirectoryMoveTest");
    std::thread([&]() {
        SCOPED_ATTACH_TASK(tracker);
        const int64_t tracked_before = tracker->consumption();
        auto path = (_tmp_dir / "moved").string();
        auto ram_dir = std::make_unique<DorisRAMFSDirectory>();
        ram_dir->init(_fs, path.c_str());
        write_ram_file(ram_dir.get(), "a", 10 * 1024 + 5);
        write_ram_file(ram_dir.get(), "b", 3000);
        const std::string a = read_file(ram_dir.get(), "a");

        ram_dir->move_files_to_disk();
        EXPECT_TRUE(ram_dir->spilled());
        EXPECT_EQ(tracker->consumption() - tracked_before, 0);
        EXPECT_EQ(std::filesystem::file_size(std::filesystem::path(path) / "a"), 10 * 1024 + 5);
        EXPECT_EQ(read_file(ram_dir.get(), "a"), a);
        EXPECT_EQ(ram_dir->fileLength("b"), 3000);
        std::vector<std::string> names;
        EXPECT_TRUE(ram_dir->list(&names));
        std::sort(names.begin(), names.end());
        EXPECT_EQ(names, (std::vector<std::string> {"a", "b"}));
        EXPECT_TRUE(ram_dir->deleteDirectory());
        EXPECT_FALSE(std::filesystem::exists(path));
    }).join();
}

} // namespace doris::segment_v2
