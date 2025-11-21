from bcc import BPF
import time, os

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

kstime = time.time_ns() - time.monotonic_ns()

_bpf_base = """
#include <uapi/linux/ptrace.h>
#include <net/sock.h>
#include <bcc/proto.h>

struct event {
        int pad;
        int size;
        u64 timestamp_ns;
};
BPF_RINGBUF_OUTPUT(events, 1 << 12);

bool send_return(struct pt_regs *ctx)
{
    u64 id = bpf_get_current_pid_tgid();
    if (id >> 32 != __PID__) { return 0; }
    int size = PT_REGS_RC(ctx);
    if (size <= 32) { return 0; }
    u64 time = bpf_ktime_get_ns();
    struct event event = {
        .size = size,
        .timestamp_ns = time
    };
    events.ringbuf_output(&event, sizeof(event), 0);
    return true;
}
"""

def get_bpf(pid):
    src = _bpf_base
    if not USE_RINGBUF:
        src = (
            src.replace(
                "BPF_RINGBUF_OUTPUT(events, 1 << 12);",
                "BPF_PERF_OUTPUT(events);"
            )
            .replace(
                "events.ringbuf_output(&event, sizeof(event), 0);",
                "events.perf_submit(ctx, &event, sizeof(event));"
            )
        )
    src = src.replace("__PID__", pid)    
    return BPF(text=src, cflags=["-Wno-macro-redefined"])

def get_call_back(bpf):
    def callback(ctx, data, size):
        event = bpf['events'].event(data)
        print("%d,%d" % (event.timestamp_ns + kstime, event.size))
    return callback