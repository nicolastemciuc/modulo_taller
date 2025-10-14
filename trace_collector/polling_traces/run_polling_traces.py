#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import signal
import sys
import time
from polling_traces import PollingTraceCollector

def main():
    parser = argparse.ArgumentParser(
        description="Collect polling traces for a given process ID and dump each feature to CSV."
    )
    parser.add_argument("pid", type=int, help="PID of the target workload process")
    parser.add_argument(
        "-i", "--interval", type=int, default=50,
        help="Polling interval in milliseconds (default: 50)"
    )
    parser.add_argument(
        "-t", "--ts-mode", choices=["monotonic_ns", "wallclock_iso"],
        default="monotonic_ns", help="Timestamp mode for CSV files"
    )
    parser.add_argument(
        "-o", "--out-dir", default=None,
        help="Optional output directory (default: auto-generated timestamped folder)"
    )

    args = parser.parse_args()

    collector = PollingTraceCollector(
        pid=args.pid,
        interval_ms=args.interval,
        write_csv=True,
        ts_mode=args.ts_mode,
        out_dir=args.out_dir,
    )

    # Handle Ctrl + C (SIGINT)
    def handle_sigint(sig, frame):
        print("\nStopping collector… flushing CSVs.")
        collector.stop()
        print("Polling traces written to:", collector.out_dir)
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sigint)

    print(f"Starting polling for PID {args.pid} (interval {args.interval} ms)…")
    collector.start()
    print("Press Ctrl +C to stop and write CSV files.\n")

    # Keep the process alive until interrupted
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        handle_sigint(None, None)

if __name__ == "__main__":
    main()
