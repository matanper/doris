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

#include <cstdint>
#include <memory>

namespace doris {

class MemTrackerLimiter;

// Net heap bytes the calling thread has allocated so far (allocated minus freed), from jemalloc's
// per-thread counters. Sanitizer builds, which do not use jemalloc, report the process-wide
// total instead. 0 when neither is available.
int64_t thread_net_allocated_bytes();

// Puts the heap memory that third-party code allocates with plain new or malloc, which Doris's
// Allocator never sees, on a memory tracker. Code that calls into such a library opens a Scope
// around each call; the scope measures the net bytes the thread allocated while it was open and
// adds them to the account. The account releases what is left when it is destroyed, so memory the
// library frees outside a scope is released at the latest then.
class UntrackedHeapAccount {
public:
    // Accounts on the memory tracker of the calling thread's task, if it has one.
    UntrackedHeapAccount();
    ~UntrackedHeapAccount();

    UntrackedHeapAccount(const UntrackedHeapAccount&) = delete;
    UntrackedHeapAccount& operator=(const UntrackedHeapAccount&) = delete;

    // Bytes currently accounted.
    int64_t bytes() const { return _bytes; }
    void add(int64_t bytes);

    class Scope {
    public:
        // `excluded` points to a counter of bytes inside the measured calls that another tracker
        // already accounts for; its growth during the scope is not added again. A null account
        // makes the scope a no-op.
        explicit Scope(UntrackedHeapAccount* account, const int64_t* excluded = nullptr);
        ~Scope();

        Scope(const Scope&) = delete;
        Scope& operator=(const Scope&) = delete;

    private:
        UntrackedHeapAccount* _account;
        const int64_t* _excluded;
        int64_t _start;
        int64_t _excluded_start;
    };

private:
    std::shared_ptr<MemTrackerLimiter> _tracker;
    int64_t _bytes = 0;
};

} // namespace doris
