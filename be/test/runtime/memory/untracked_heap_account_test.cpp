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

#include "runtime/memory/untracked_heap_account.h"

#include <gtest/gtest.h>

#include <memory>
#include <thread>
#include <vector>

#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"

namespace doris {

namespace {
constexpr int64_t kMB = 1 << 20;
// Other threads of the test process may allocate while a sanitizer build counts process-wide.
constexpr int64_t kSlack = 256 << 10;
} // namespace

TEST(UntrackedHeapAccountTest, ConsumesWhatScopesAllocateAndReleasesTheRest) {
    if (thread_net_allocated_bytes() == 0) {
        GTEST_SKIP() << "no allocation counter in this build";
    }
    auto tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER, "UntrackedHeapAccountTest");
    std::thread([&]() {
        SCOPED_ATTACH_TASK(tracker);
        const int64_t before = tracker->consumption();
        std::vector<std::unique_ptr<char[]>> kept;
        {
            UntrackedHeapAccount account;
            {
                UntrackedHeapAccount::Scope scope(&account);
                for (int i = 0; i < 8; ++i) {
                    kept.emplace_back(new char[kMB]);
                    kept.back()[0] = 1;
                }
            }
            EXPECT_NEAR(account.bytes(), 8 * kMB, kSlack);
            EXPECT_NEAR(tracker->consumption() - before, 8 * kMB, kSlack);

            {
                UntrackedHeapAccount::Scope scope(&account);
                kept.resize(2);
            }
            EXPECT_NEAR(account.bytes(), 2 * kMB, kSlack);

            // Bytes another tracker already accounts for are not added again.
            int64_t excluded = 0;
            {
                UntrackedHeapAccount::Scope scope(&account, &excluded);
                kept.emplace_back(new char[kMB]);
                excluded += kMB;
            }
            EXPECT_NEAR(account.bytes(), 2 * kMB, kSlack);
            kept.clear();
        }
        // The account releases what is left when it is destroyed, and never more than it took.
        EXPECT_EQ(tracker->consumption(), before);
    }).join();
}

TEST(UntrackedHeapAccountTest, NullAccountScopeDoesNothing) {
    UntrackedHeapAccount::Scope scope(nullptr);
    std::unique_ptr<char[]> buffer(new char[kMB]);
    buffer[0] = 1;
}

} // namespace doris
