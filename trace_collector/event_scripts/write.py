#!/usr/bin/env python
import time, argparse, sys
from bcc import BPF
from send import get_call_back, USE_RINGBUF

kstime = time.time_ns() - time.monotonic_ns()

bpf_text = """
#include <uapi/linux/ptrace.h>
#include <net/sock.h>
#include <bcc/proto.h>

struct event {
        int pad;
        int size;
        u64 timestamp_ns;
};

struct write_event {
    bool sock_event;
};

BPF_RINGBUF_OUTPUT(events, 1 << 12);
BPF_HASH(write_events, uint64_t, struct write_event);

int syscall__probe_entry_write(struct pt_regs* ctx, int fd, char* buf, size_t count) {
    uint64_t id = bpf_get_current_pid_tgid();
    if (id >> 32 != __PID__) { return 0; }
    struct write_event wevent = {};
    wevent.sock_event = false;
    write_events.update(&id, &wevent);
    return 0;
}

int probe_entry_security_socket_sendmsg(struct pt_regs *ctx)
{
    uint64_t id = bpf_get_current_pid_tgid();
    struct write_event* wevent = write_events.lookup(&id);
    if (wevent != NULL) {
        wevent->sock_event = true;
    }
    return 0;
}

int syscall__probe_ret_write(struct pt_regs* ctx) {
    uint64_t id = bpf_get_current_pid_tgid();
    struct write_event* wevent = write_events.lookup(&id);
    if (wevent != NULL && wevent->sock_event) {
        int size = PT_REGS_RC(ctx);
        u64 time = bpf_ktime_get_ns();
        struct event event = {
            .size = size,
            .timestamp_ns = time
        };
        events.ringbuf_output(&event, sizeof(event), 0);
    }
    write_events.delete(&id);
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

parser = argparse.ArgumentParser()
parser.add_argument("-p", "--pid", default=None)
args = parser.parse_args()

if args.pid is None:
    print("PID must set")
    exit(0)

bpf_text = bpf_text.replace("__PID__", args.pid)

bpf_obj = BPF(text=bpf_text, cflags=["-Wno-macro-redefined"])
bpf_obj.attach_kprobe(
    event="security_socket_sendmsg",
    fn_name="probe_entry_security_socket_sendmsg"
)
bpf_obj.attach_kprobe(
    event="__x64_sys_write",
    fn_name="syscall__probe_entry_write"
)
bpf_obj.attach_kretprobe(
    event="__x64_sys_write",
    fn_name="syscall__probe_ret_write"
)
cb = get_call_back(bpf_obj)
if USE_RINGBUF:
    bpf_obj["events"].open_ring_buffer(cb)
else:
    bpf_obj["events"].open_perf_buffer(cb)

while True:
    try:
        if USE_RINGBUF:
            bpf_obj.ring_buffer_consume()
        else:
            bpf_obj.perf_buffer_poll()
    except KeyboardInterrupt:
        exit()
    sys.stdout.flush()