#!/usr/bin/env python3
from bcc import BPF
import argparse, sys, time
from mem import get_bpf_source, attach_probes

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
cudart = "/home/smartlab/.local/lib/python3.10/site-packages/nvidia/nccl/lib/libnccl.so.2"

attach_probes(bpf, "ncclAllReduce", name=cudart, pid=pid)

def callback(ctx, data, size):
    event = bpf['events'].event(data)
    print("%d,%d" % (event.timestamp_ns + kstime, event.size))

bpf["events"].open_ring_buffer(callback)

while True:
    try:
        bpf.ring_buffer_consume()
    except KeyboardInterrupt:
        exit()
    sys.stdout.flush()