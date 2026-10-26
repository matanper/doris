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

#include "common/raw_crash_dump.h"

#include <gtest/gtest.h>

#include <csignal>
#include <cstdint>
#include <thread>

#include "runtime/thread_context.h" // signal_handler.h relies on what it includes
// clang-format off
#include "common/signal_handler.h"
// clang-format on

namespace doris::signal {

namespace {

volatile bool g_keep_recursing = true;
int* volatile g_null = nullptr;

[[gnu::noinline]] void fault_here() {
    *g_null = 1;
}

[[gnu::noinline]] void nested_fault(int depth) {
    if (depth == 0) {
        fault_here();
    } else {
        nested_fault(depth - 1);
    }
    asm volatile("" ::: "memory");
}

// Faults with a frame pointer that points nowhere, as a smashed stack leaves it.
[[gnu::noinline]] void fault_with_corrupt_fp() {
#if defined(__aarch64__)
    asm volatile("mov x29, %0\n\tmov x1, #0\n\tldr x1, [x1]\n" ::"r"(0xdeadbeef0UL)
                 : "x1", "memory");
#elif defined(__x86_64__)
    asm volatile("mov %0, %%rbp\n\tmovq 0, %%rax\n" ::"r"(0xdeadbeef0UL) : "rax", "memory");
#endif
}

[[gnu::noinline]] int recurse(int n) {
    volatile char frame[1024];
    frame[n % 1024] = static_cast<char>(n);
    if (g_keep_recursing) {
        return recurse(n + 1) + frame[n % 1024];
    }
    return frame[0];
}

void run_in_thread(void (*fn)()) {
    std::thread thread([fn] {
        install_thread_alt_stack();
        fn();
    });
    thread.join();
}

} // namespace

TEST(RawCrashDumpTest, ThreadAltStackIsInstalledOnce) {
    std::thread thread([] {
        install_thread_alt_stack();
        stack_t first {};
        ASSERT_EQ(sigaltstack(nullptr, &first), 0);
        EXPECT_EQ(first.ss_flags & SS_DISABLE, 0);
        EXPECT_EQ(first.ss_size, 256 * 1024);
        install_thread_alt_stack();
        stack_t second {};
        ASSERT_EQ(sigaltstack(nullptr, &second), 0);
        EXPECT_EQ(second.ss_sp, first.ss_sp);
    });
    thread.join();
}

// A SIGSEGV prints the registers of the faulting frame and the return addresses of its callers,
// each with its offset in the executable, before the symbolized trace.
TEST(RawCrashDumpDeathTest, SegvPrintsRegistersAndCallerFrames) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(
            {
                InstallFailureSignalHandler();
                run_in_thread([] { nested_fault(5); });
            },
            ::testing::KilledBySignal(SIGSEGV),
            "raw crash context: signal 11 .*fault_addr 0x0 .*altstack yes .*"
            "pc 0x[0-9a-f]+ [^ ]+\\+0x[0-9a-f]+.*raw #6 0x[0-9a-f]+ [^ ]+\\+0x[0-9a-f]+");
}

// With a frame pointer outside the stack, the registers are still printed and the walk stops.
TEST(RawCrashDumpDeathTest, CorruptFramePointerStopsTheWalk) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(
            {
                InstallFailureSignalHandler();
                run_in_thread(fault_with_corrupt_fp);
            },
            ::testing::KilledBySignal(SIGSEGV),
            "raw crash context: signal 11 .*pc 0x[0-9a-f]+ [^ ]+\\+0x[0-9a-f]+.*"
            "fp 0xdeadbeef0.*raw frame walk stopped: fp outside the thread's stack at fp "
            "0xdeadbeef0");
}

// A thread that overflows its stack still gets its registers printed, from the alternate stack.
TEST(RawCrashDumpDeathTest, StackOverflowIsReportedFromTheAltStack) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(
            {
                InstallFailureSignalHandler();
                run_in_thread([] { static_cast<void>(recurse(0)); });
            },
            ::testing::KilledBySignal(SIGSEGV),
            "raw crash context: signal 11 .*altstack yes .*pc 0x[0-9a-f]+ [^ ]+\\+0x[0-9a-f]+");
}

} // namespace doris::signal
