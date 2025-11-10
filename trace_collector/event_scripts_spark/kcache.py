#!/usr/bin/env python3
import time
import sys
import argparse
from bcc import BPF
from bcc.utils import printb

kstime = time.time_ns() - time.monotonic_ns()

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
            void *freelist;
            void *s_mem;
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
                    int slabs;
                };
#endif
            };
            void *freelist;
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
    void *freelist;
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

BPF_PERF_OUTPUT(events);

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
    events.perf_submit(ctx, &event, sizeof(event));

    return 0;
}
"""

if args.pid is None:
    print("PID must set")
    exit(0)

bpf_text = bpf_text.replace("__PID__", args.pid)
bpf = BPF(text=bpf_text, cflags=["-Wno-macro-redefined"])

def callback(ctx, data, size):
    event = bpf['events'].event(data)
    print("%d,%d" % (event.timestamp_ns + kstime, event.size))


bpf['events'].open_perf_buffer(callback)

while True:
    try:
       bpf.perf_buffer_poll()
    except KeyboardInterrupt:
        exit()
    sys.stdout.flush()