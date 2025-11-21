#!/usr/bin/env python3
from bcc import BPF
import argparse, sys, time
from mem import get_bpf_source, attach_probes, USE_RINGBUF

kstime = time.time_ns() - time.monotonic_ns()

bpf_source = get_bpf_source()

parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter
)
parser.add_argument("-p", "--pid", type=int, default=-1)
parser.add_argument("-s", "--sample-rate", default=50000, type=int)

args = parser.parse_args()

pid = args.pid
sample_every_n = args.sample_rate

bpf_source = bpf_source.replace("SAMPLE_EVERY_N", str(sample_every_n))

bpf = BPF(text=bpf_source, cflags=["-Wno-macro-redefined"])

attach_probes(bpf, "malloc", pid=pid)

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