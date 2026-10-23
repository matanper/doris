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

#include <csignal>

namespace doris::signal {

// Records the executable's load address and name, and gives the calling thread an alternate
// signal stack. Call once at startup, before the failure signal handler is installed.
void init_raw_crash_dump();

// Gives the calling thread an alternate signal stack, so that the failure signal handler still
// runs when the thread's own stack is overflowed or smashed, and records the thread's stack
// bounds for the frame walk. Idempotent; the stack is released when the thread exits.
void install_thread_alt_stack();

// Async-signal-safe. Writes to fd the faulting thread's registers from ucontext (pc, lr, fp, sp)
// and the fault address, then the return addresses found by walking frame pointers from the
// ucontext's fp. An address inside the executable is also printed as <exe>+0x<offset>, which
// addr2line resolves against the same binary. Every frame pointer is checked against the
// thread's stack and read without faulting; the walk stops at the first bad one.
void dump_raw_crash_context(int fd, int signal_number, const siginfo_t* info, const void* ucontext);

} // namespace doris::signal
