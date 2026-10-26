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

#include <link.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <ucontext.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>

namespace doris::signal {

namespace {

// Load bias and [lo, hi) of the executable's loaded segments, and its file name.
uintptr_t g_exe_bias = 0;
uintptr_t g_exe_lo = 0;
uintptr_t g_exe_hi = 0;
char g_exe_name[64] = "exe";

// Large enough for the failure signal handler's later, unsafe part (symbolized stack trace, log
// flush), which runs on the same stack.
constexpr size_t kAltStackSize = 256 * 1024;
constexpr int kMaxFrames = 64;

// The calling thread's stack bounds, [lo, hi), and its alternate signal stack. Trivially
// constructible and destructible, so the signal handler reads it without the lazy initialization
// a thread_local with a destructor would run on first use.
struct ThreadStack {
    uintptr_t lo;
    uintptr_t hi;
    void* alt;
    size_t alt_size;
};
thread_local ThreadStack t_stack {};

// Releases the alternate stack when the thread exits. Constructed only by
// install_thread_alt_stack, never in the signal handler.
struct AltStackReleaser {
    ~AltStackReleaser() {
        if (t_stack.alt == nullptr) {
            return;
        }
        stack_t disable {};
        disable.ss_flags = SS_DISABLE;
        if (sigaltstack(&disable, nullptr) == 0) {
            munmap(t_stack.alt, t_stack.alt_size);
            t_stack.alt = nullptr;
        }
    }
};
thread_local AltStackReleaser t_alt_stack_releaser;

int find_executable(struct dl_phdr_info* info, size_t, void*) {
    // The first object reported is the executable.
    g_exe_bias = info->dlpi_addr;
    uintptr_t lo = UINTPTR_MAX;
    uintptr_t hi = 0;
    for (int i = 0; i < info->dlpi_phnum; ++i) {
        const auto& phdr = info->dlpi_phdr[i];
        if (phdr.p_type != PT_LOAD) {
            continue;
        }
        lo = std::min<uintptr_t>(lo, info->dlpi_addr + phdr.p_vaddr);
        hi = std::max<uintptr_t>(hi, info->dlpi_addr + phdr.p_vaddr + phdr.p_memsz);
    }
    if (hi > lo) {
        g_exe_lo = lo;
        g_exe_hi = hi;
    }
    return 1;
}

// A fixed buffer formatted without allocating, written with write(2).
class Line {
public:
    explicit Line(int fd) : _fd(fd) {}
    ~Line() { flush(); }

    Line& str(const char* s) {
        while (*s != '\0') {
            put(*s++);
        }
        return *this;
    }
    Line& hex(uint64_t v) {
        char digits[16];
        int n = 0;
        do {
            digits[n++] = "0123456789abcdef"[v & 0xf];
            v >>= 4;
        } while (v != 0);
        str("0x");
        while (n > 0) {
            put(digits[--n]);
        }
        return *this;
    }
    Line& dec(uint64_t v) {
        char digits[20];
        int n = 0;
        do {
            digits[n++] = static_cast<char>('0' + v % 10);
            v /= 10;
        } while (v != 0);
        while (n > 0) {
            put(digits[--n]);
        }
        return *this;
    }
    // The address, and where it is in the executable when it is inside it.
    Line& addr(uint64_t a) {
        hex(a);
        if (a >= g_exe_lo && a < g_exe_hi) {
            str(" ").str(g_exe_name).str("+").hex(a - g_exe_bias);
        }
        return *this;
    }
    void flush() {
        size_t done = 0;
        while (done < _len) {
            ssize_t n = ::write(_fd, _buf + done, _len - done);
            if (n <= 0) {
                if (n < 0 && errno == EINTR) {
                    continue;
                }
                break;
            }
            done += static_cast<size_t>(n);
        }
        _len = 0;
    }

private:
    void put(char c) {
        if (_len == sizeof(_buf)) {
            flush();
        }
        _buf[_len++] = c;
    }

    int _fd;
    char _buf[512];
    size_t _len = 0;
};

// A return address saved in a frame record, or in lr, can carry a pointer authentication code in
// its top bits on aarch64; user addresses fit in 48 bits.
uint64_t strip_pac(uint64_t address) {
#if defined(__aarch64__)
    return address & ((1ULL << 48) - 1);
#else
    return address;
#endif
}

// Reads n bytes at addr without faulting on an unmapped address.
bool read_memory(uintptr_t addr, void* out, size_t n) {
    struct iovec local {out, n};
    struct iovec remote {reinterpret_cast<void*>(addr), n};
    return syscall(SYS_process_vm_readv, getpid(), &local, 1, &remote, 1, 0) ==
           static_cast<ssize_t>(n);
}

struct Registers {
    uint64_t pc = 0;
    uint64_t lr = 0; // 0 where the architecture keeps the return address on the stack
    uint64_t fp = 0;
    uint64_t sp = 0;
};

Registers registers_of(const void* ucontext) {
    Registers r;
    if (ucontext == nullptr) {
        return r;
    }
    const auto* uc = static_cast<const ucontext_t*>(ucontext);
#if defined(__aarch64__)
    r.pc = uc->uc_mcontext.pc;
    r.lr = strip_pac(uc->uc_mcontext.regs[30]);
    r.fp = uc->uc_mcontext.regs[29];
    r.sp = uc->uc_mcontext.sp;
#elif defined(__x86_64__)
    r.pc = static_cast<uint64_t>(uc->uc_mcontext.gregs[REG_RIP]);
    r.fp = static_cast<uint64_t>(uc->uc_mcontext.gregs[REG_RBP]);
    r.sp = static_cast<uint64_t>(uc->uc_mcontext.gregs[REG_RSP]);
#endif
    return r;
}

} // namespace

void init_raw_crash_dump() {
    dl_iterate_phdr(find_executable, nullptr);
    char path[512];
    ssize_t n = readlink("/proc/self/exe", path, sizeof(path) - 1);
    if (n > 0) {
        path[n] = '\0';
        const char* base = strrchr(path, '/');
        base = base == nullptr ? path : base + 1;
        strncpy(g_exe_name, base, sizeof(g_exe_name) - 1);
        g_exe_name[sizeof(g_exe_name) - 1] = '\0';
    }
    install_thread_alt_stack();
}

void install_thread_alt_stack() {
    if (t_stack.alt != nullptr) {
        return;
    }
    pthread_attr_t attr;
    if (pthread_getattr_np(pthread_self(), &attr) == 0) {
        void* stack_addr = nullptr;
        size_t stack_size = 0;
        if (pthread_attr_getstack(&attr, &stack_addr, &stack_size) == 0) {
            t_stack.lo = reinterpret_cast<uintptr_t>(stack_addr);
            t_stack.hi = t_stack.lo + stack_size;
        }
        pthread_attr_destroy(&attr);
    }
    // One guard page below the alternate stack catches the handler overflowing it.
    const size_t page = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    const size_t size = kAltStackSize + page;
    void* mem = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE | MAP_STACK, -1, 0);
    if (mem == MAP_FAILED) {
        return;
    }
    mprotect(mem, page, PROT_NONE);
    stack_t ss {};
    ss.ss_sp = static_cast<char*>(mem) + page;
    ss.ss_size = kAltStackSize;
    ss.ss_flags = 0;
    if (sigaltstack(&ss, nullptr) != 0) {
        munmap(mem, size);
        return;
    }
    t_stack.alt = mem;
    t_stack.alt_size = size;
    static_cast<void>(&t_alt_stack_releaser);
}

void dump_raw_crash_context(int fd, int signal_number, const siginfo_t* info,
                            const void* ucontext) {
    const Registers r = registers_of(ucontext);
    const uintptr_t stack_lo = t_stack.lo;
    const uintptr_t stack_hi = t_stack.hi;
    {
        Line line(fd);
        line.str("*** raw crash context: signal ").dec(static_cast<uint64_t>(signal_number));
        line.str(" tid ").dec(static_cast<uint64_t>(syscall(SYS_gettid)));
        line.str(" fault_addr ")
                .hex(info != nullptr ? reinterpret_cast<uintptr_t>(info->si_addr) : 0);
        line.str(" exe ").str(g_exe_name).str(" [").hex(g_exe_lo).str(", ").hex(g_exe_hi);
        line.str(") stack [").hex(stack_lo).str(", ").hex(stack_hi).str(") altstack ");
        line.str(t_stack.alt != nullptr ? "yes" : "no").str(" ***\n");
    }
    {
        Line line(fd);
        line.str("  pc ").addr(r.pc).str("\n");
        line.str("  lr ").addr(r.lr).str("\n");
        line.str("  fp ").hex(r.fp).str("\n");
        line.str("  sp ").hex(r.sp).str("\n");
    }
    {
        Line line(fd);
        line.str("  raw #0 ").addr(r.pc).str("\n");
        if (r.lr != 0) {
            line.str("  raw #1 ").addr(r.lr).str(" (lr)\n");
        }
    }
    // Each frame record is {previous fp, return address} at fp, on aarch64 and x86_64.
    uint64_t fp = r.fp;
    const char* stop = "depth limit";
    for (int depth = 0; depth < kMaxFrames; ++depth) {
        const bool in_stack = stack_hi > stack_lo ? fp >= stack_lo && fp + 16 <= stack_hi
                                                  : fp >= r.sp && fp - r.sp < (1UL << 30);
        if (fp == 0) {
            stop = "fp is 0";
            break;
        }
        if ((fp & 0x7) != 0 || !in_stack) {
            stop = "fp outside the thread's stack";
            break;
        }
        uint64_t record[2];
        if (!read_memory(fp, record, sizeof(record))) {
            stop = "fp unreadable";
            break;
        }
        Line line(fd);
        line.str("  raw #").dec(static_cast<uint64_t>(depth) + 2).str(" ").addr(strip_pac(record[1]));
        line.str(" (fp ").hex(fp).str(")\n");
        line.flush();
        if (record[0] <= fp) {
            stop = "next fp is not above this one";
            break;
        }
        fp = record[0];
    }
    Line line(fd);
    line.str("*** raw frame walk stopped: ").str(stop).str(" at fp ").hex(fp).str(" ***\n");
}

} // namespace doris::signal
