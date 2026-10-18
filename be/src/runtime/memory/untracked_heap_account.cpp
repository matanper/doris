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

#include <bvar/reducer.h>

#include <algorithm>

#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"

#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
#include <sanitizer/allocator_interface.h>
#elif defined(USE_JEMALLOC)
#include "jemalloc/jemalloc.h"
#endif

namespace doris {

namespace {
bvar::Adder<int64_t> g_untracked_heap_account_bytes("untracked_heap_account", "bytes");
} // namespace

int64_t thread_net_allocated_bytes() {
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
    return static_cast<int64_t>(__sanitizer_get_current_allocated_bytes());
#elif defined(USE_JEMALLOC)
    // jemalloc hands out pointers to the thread's own counters, so reading them later is a load.
    thread_local uint64_t* allocated = nullptr;
    thread_local uint64_t* deallocated = nullptr;
    thread_local bool unavailable = false;
    if (allocated == nullptr) {
        if (unavailable) {
            return 0;
        }
        size_t size = sizeof(uint64_t*);
        if (jemallctl("thread.allocatedp", &allocated, &size, nullptr, 0) != 0 ||
            jemallctl("thread.deallocatedp", &deallocated, &size, nullptr, 0) != 0) {
            allocated = nullptr;
            unavailable = true;
            return 0;
        }
    }
    return static_cast<int64_t>(*allocated - *deallocated);
#else
    return 0;
#endif
}

UntrackedHeapAccount::UntrackedHeapAccount() {
    if (doris::pthread_context_ptr_init || bthread_self() != 0) {
        _tracker = thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker_sptr();
    }
}

UntrackedHeapAccount::~UntrackedHeapAccount() {
    add(-_bytes);
}

void UntrackedHeapAccount::add(int64_t bytes) {
    // Memory the library frees later than it allocated it can make a scope negative; the account
    // never releases more than it consumed.
    bytes = std::max(bytes, -_bytes);
    if (bytes == 0) {
        return;
    }
    _bytes += bytes;
    g_untracked_heap_account_bytes << bytes;
    if (_tracker) {
        _tracker->consume(bytes);
    }
}

UntrackedHeapAccount::Scope::Scope(UntrackedHeapAccount* account, const int64_t* excluded)
        : _account(account),
          _excluded(excluded),
          _start(thread_net_allocated_bytes()),
          _excluded_start(excluded ? *excluded : 0) {}

UntrackedHeapAccount::Scope::~Scope() {
    if (_account == nullptr) {
        return;
    }
    int64_t bytes = thread_net_allocated_bytes() - _start;
    if (_excluded != nullptr) {
        bytes -= *_excluded - _excluded_start;
    }
    _account->add(bytes);
}

} // namespace doris
