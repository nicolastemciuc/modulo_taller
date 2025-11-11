#!/usr/bin/env python3
import time, sys, argparse, os
from bcc import BPF

kstime = time.time_ns() - time.monotonic_ns()

# Detect if the kernel supports ring buffer maps (5.8+)
def _kernel_supports_ringbuf():
    rel = os.uname().release
    rel = rel.split("-", 1)[0]
    parts = rel.split(".")
    try:
        major = int(parts[0])
        minor = int(parts[1]) if len(parts) > 1 else 0
    except ValueError:
        return True

    return major > 5 or (major == 5 and minor >= 8)

USE_RINGBUF = _kernel_supports_ringbuf()

parser = argparse.ArgumentParser()
parser.add_argument("-p", "--pid", default=None)
args = parser.parse_args()

# define BPF program
bpf_text = """
#include <uapi/linux/ptrace.h>
#include <linux/mm.h>
#include <linux/kasan.h>

struct memcg_cache_params {};

struct slab {
    unsigned long __page_flags;

#if defined(CONFIG_SLAB)

    struct kmem_cache *slab_cache;
    union {
        struct {
            struct list_head slab_list;
            void *freelist; /* array of free object indexes */
            void *s_mem;    /* first object */
        };
        struct rcu_head rcu_head;
    };
    unsigned int active;

#elif defined(CONFIG_SLUB)

    struct kmem_cache *slab_cache;
    union {
        struct {
            union {
                struct list_head slab_list;
#ifdef CONFIG_SLUB_CPU_PARTIAL
                struct {
                    struct slab *next;
                        int slabs;      /* Nr of slabs left */
                };
#endif
            };
            /* Double-word boundary */
            void *freelist;         /* first free object */
            union {
                unsigned long counters;
                struct {
                    unsigned inuse:16;
                    unsigned objects:15;
                    unsigned frozen:1;
                };
            };
        };
        struct rcu_head rcu_head;
    };
    unsigned int __unused;

#elif defined(CONFIG_SLOB)

    struct list_head slab_list;
    void *__unused_1;
    void *freelist;         /* first free block */
    long units;
    unsigned int __unused_2;

#else
#error "Unexpected slab allocator configured"
#endif

    atomic_t __page_refcount;
#ifdef CONFIG_MEMCG
    unsigned long memcg_data;
#endif
};

// slab_address() will not be used, and NULL will be returned directly, which
// can avoid adaptation of different kernel versions
static inline void *slab_address(const struct slab *slab)
{
    return NULL;
}

struct kmem_cache {
    unsigned int size;
};

struct event {
        int pad;
        int size;
        u64 timestamp_ns;
};
BPF_RINGBUF_OUTPUT(events, 1 << 12);

int kprobe__kmem_cache_alloc(struct pt_regs *ctx, struct kmem_cache *cachep)
{
    u64 id = bpf_get_current_pid_tgid();
    if (id >> 32 != __PID__) { return 0; }
    int size = cachep->size;
    u64 time = bpf_ktime_get_ns();
    struct event event = {
        .size = size,
        .timestamp_ns = time
    };
    events.ringbuf_output(&event, sizeof(event), 0);

    return 0;
}
"""

if not USE_RINGBUF:
    bpf_text = (
        bpf_text.replace(
            "BPF_RINGBUF_OUTPUT(events, 1 << 12);",
            "BPF_PERF_OUTPUT(events);"
        )
        .replace(
            "events.ringbuf_output(&event, sizeof(event), 0);",
            "events.perf_submit(ctx, &event, sizeof(event));"
        )
    )

if args.pid is None:
    print("PID must set")
    exit(0)

bpf_text = bpf_text.replace("__PID__", args.pid)
bpf = BPF(text=bpf_text, cflags=["-Wno-macro-redefined"])

def callback(ctx, data, size):
    event = bpf['events'].event(data)
    print("%d,%d" % (event.timestamp_ns + kstime, event.size))


if USE_RINGBUF:
    bpf["events"].open_ring_buffer(callback)
else:
    bpf["events"].open_perf_buffer(callback)

while True:
    try:
        if USE_RINGBUF:
            bpf.ring_buffer_consume()
        else:
            bpf.perf_buffer_poll()
    except KeyboardInterrupt:
        exit()
    sys.stdout.flush()