#!/usr/bin/env python3
from bcc import BPF
import argparse, sys, time
from mem import get_bpf_source, attach_probes

kstime = time.time_ns() - time.monotonic_ns()

parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter
)
parser.add_argument("-p", "--pid", type=int, default=-1)
parser.add_argument("-s", "--sample-rate", default=50000, type=int)
args = parser.parse_args()
pid = args.pid
sample_every_n = args.sample_rate

bpf_source = get_bpf_source()
bpf_source = bpf_source.replace("SAMPLE_EVERY_N", str(sample_every_n))
bpf = BPF(text=bpf_source, cflags=["-Wno-macro-redefined"])

cudart = "/home/smartlab/.local/lib/python3.10/site-packages/nvidia/cuda_runtime/lib/libcudart.so.12"
attach_probes(bpf, "cudaMalloc", name=cudart)
attach_probes(bpf, "cudaMemcpy", name=cudart)
attach_probes(bpf, "cudaMallocAsync", name=cudart)
attach_probes(bpf, "cudaMemcpyAsync", name=cudart)


def callback(ctx, data, size):
    event = bpf['events'].event(data)
    print("%d,%d" % (event.timestamp_ns + kstime, event.size))

bpf['events'].open_ring_buffer(callback)

while True:
    try:
        bpf.ring_buffer_consume()
    except KeyboardInterrupt:
        exit()
    sys.stdout.flush()