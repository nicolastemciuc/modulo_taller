#!/usr/bin/env python3
import argparse
import csv
import re
import subprocess
import sys

TCPDUMP_CMD = "tcpdump_pfring"

# With -tt, tcpdump prints UNIX epoch seconds.microseconds at the start
LINE_RE = re.compile(
    r'^(?P<ts_epoch>\d+\.\d+)\s+IP\s+'
    r'(?P<src_ip>\d+\.\d+\.\d+\.\d+)\.(?P<src_port>\d+)\s+>\s+'
    r'(?P<dst_ip>\d+\.\d+\.\d+\.\d+)\.(?P<dst_port>\d+):.*length\s+(?P<length>\d+)'
)

DEFAULT_FILTER = (
    "not port 22 and ip and tcp and "
    "((ip[2:2]-((ip[0]&0x0f)<<2)-((tcp[12]&0xf0)>>2))>0)"
)

def run_capture(iface: str, bpf_filter: str, csv_path: str, sudo: bool):
    with open(csv_path, "w", newline="", encoding="utf-8") as csv_f:
        csv_w = csv.writer(csv_f)
        # NOTE: timestamp is now epoch-derived; we expose both ns and seconds for convenience
        csv_w.writerow([
            "ts_ns", "timestamp_epoch_s",  # NEW absolute time columns
            "source_ip", "source_port",
            "destination_ip", "destination_port",
            "packet_size"
        ])

        cmd = []
        if sudo:
            cmd.append("sudo")
        # -tt => print epoch seconds.microseconds at line start
        cmd += [TCPDUMP_CMD, "-tt", "-ni", iface, "-l", bpf_filter]

        print(f"[*] Capturing packets on {iface} -> {csv_path}")
        print(f"[*] Command: {' '.join(cmd)}")

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        try:
            assert proc.stdout is not None
            for line in proc.stdout:
                m = LINE_RE.search(line)
                if not m:
                    continue
                ts_epoch = float(m.group("ts_epoch"))
                ts_ns = int(ts_epoch * 1_000_000_000)

                csv_w.writerow([
                    ts_ns,
                    f"{ts_epoch:.6f}",
                    m.group("src_ip"),
                    m.group("src_port"),
                    m.group("dst_ip"),
                    m.group("dst_port"),
                    m.group("length"),
                ])
                csv_f.flush()
        except KeyboardInterrupt:
            print("\n[!] Interrupted by user, stopping…")
        finally:
            try:
                proc.terminate()
                proc.wait(timeout=3)
            except Exception:
                proc.kill()
            print("[*] Done.")

def main():
    ap = argparse.ArgumentParser(description="Capture packets with tcpdump_pfring and output to CSV (with ts_ns).")
    ap.add_argument("--iface", "-i", default="ens3", help="Network interface (default: ens3)")
    ap.add_argument("--filter", "-f", default=DEFAULT_FILTER, help="BPF filter expression")
    ap.add_argument("--output", "-o", default="packet_traces.csv", help="CSV output file (default: packet_traces.csv)")
    ap.add_argument("--no-sudo", action="store_true", help="Run without sudo")
    args = ap.parse_args()

    run_capture(
        iface=args.iface,
        bpf_filter=args.filter,
        csv_path=args.output,
        sudo=not args.no_sudo,
    )

if __name__ == "__main__":
    main()
