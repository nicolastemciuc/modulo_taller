from bcc import BPF
import sys

def get_bpf_source():
    return """
#include <uapi/linux/ptrace.h>

struct event {
        u64 size;
        u64 timestamp_ns;
};

BPF_ARRAY(sizes, u64, 1);
BPF_ARRAY(times, u64, 1);
BPF_PERF_OUTPUT(events);

static inline int gen_alloc_enter(struct pt_regs *ctx, size_t size) {
    int key = 0;
    u64 zero = 0;
    u64 this_size = size;
    u64* prev_ts = times.lookup(&key);
    u64 ts = bpf_ktime_get_ns();
    u64* f_size = sizes.lookup(&key);
    if (!f_size) {
        sizes.update(&key, &this_size);
        return 0;
    } else {
        __sync_fetch_and_add(f_size, this_size);
    }
    if (!prev_ts) {
        times.update(&key, &zero);
        return 0;
    }
    if (ts - *prev_ts >= SAMPLE_EVERY_N) {
        struct event event = {};
        event.size = *f_size + this_size;
        event.timestamp_ns = *prev_ts;
        events.perf_submit(ctx, &event, sizeof(event));

        times.update(&key, &ts);
        sizes.update(&key, &zero);
    }
    return 0;
}

static inline int nccl_alloc_enter(struct pt_regs *ctx, size_t size) {
    struct event event = {};
    event.size = size;
    event.timestamp_ns = bpf_ktime_get_ns();
    events.ringbuf_output(&event, sizeof(event), 0);
    return 0;
}

int cudaMalloc_enter(struct pt_regs *ctx) {
    return gen_alloc_enter(ctx, PT_REGS_PARM2(ctx));
}

int cudaMallocAsync_enter(struct pt_regs *ctx) {
    return gen_alloc_enter(ctx, PT_REGS_PARM2(ctx));
}

int cudaMemcpy_enter(struct pt_regs *ctx) {
    return gen_alloc_enter(ctx, PT_REGS_PARM3(ctx));
}

int cudaMemcpyAsync_enter(struct pt_regs *ctx) {
    return gen_alloc_enter(ctx, PT_REGS_PARM3(ctx));
}

int cudaHostAlloc_enter(struct pt_regs *ctx) {
    return gen_alloc_enter(ctx, PT_REGS_PARM2(ctx));
}

int malloc_enter(struct pt_regs *ctx) {
    return gen_alloc_enter(ctx, PT_REGS_PARM1(ctx));
}

int ncclAllReduce_enter(struct pt_regs *ctx) {
    u64 count = PT_REGS_PARM3(ctx);
    u64 datatype = PT_REGS_PARM4(ctx);
    u64 size_bytes = count;
    if (datatype == 0)       // ncclChar
    size_bytes = count * 1;
    else if (datatype == 1 || datatype == 2)  // ncclInt, ncclFloat
        size_bytes = count * 4;
    else if (datatype == 3 || datatype == 4)  // ncclDouble, ncclInt64
        size_bytes = count * 8;
    else if (datatype == 5)  // ncclHalf
        size_bytes = count * 2;
    else
    size_bytes = count; // fallback
    return gen_alloc_enter(ctx, size_bytes);
}
"""

def attach_probes(bpf, 
                  sym, 
                  fn_prefix=None, 
                  can_fail=False, 
                  name="/usr/lib/x86_64-linux-gnu/libc.so.6", 
                  pid=-1):

    if fn_prefix is None:
        fn_prefix = sym
    try:
        bpf.attach_uprobe(name=name, sym=sym, fn_name=fn_prefix + "_enter", pid=pid)
    except Exception:
        if can_fail:
            return
        else:
            raise