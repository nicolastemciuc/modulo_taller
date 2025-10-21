#!/usr/bin/env python3
import argparse
import csv
import re
import subprocess
import sys

TCPDUMP_CMD = "tcpdump_pfring"

LINE_RE = re.compile(
    r'(?P<timestamp>\d+:\d+:\d+\.\d+)\s+IP\s+'
    r'(?P<src_ip>\d+\.\d+\.\d+\.\d+)\.(?P<src_port>\d+)\s+>\s+'
    r'(?P<dst_ip>\d+\.\d+\.\d+\.\d+)\.(?P<dst_port>\d+):.*length\s+(?P<length>\d+)'
)

DEFAULT_FILTER = (
    "not port 22 and ip and tcp and "
    "((ip[2:2]-((ip[0]&0x0f)<<2)-((tcp[12]&0xf0)>>2))>0)"
)

def run_capture(iface: str, bpf_filter: str, csv_path: str, sudo: bool):
    # Open CSV for overwrite (mode "w" truncates)
    with open(csv_path, "w", newline="", encoding="utf-8") as csv_f:
        csv_w = csv.writer(csv_f)
        csv_w.writerow([
            "timestamp", "source_ip", "source_port",
            "destination_ip", "destination_port", "packet_size"
        ])

        cmd = []
        if sudo:
            cmd.append("sudo")
        cmd += [TCPDUMP_CMD, "-ni", iface, "-l", bpf_filter]

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
                if m:
                    csv_w.writerow([
                        m.group("timestamp"),
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
    ap = argparse.ArgumentParser(description="Capture packets with tcpdump_pfring and output to CSV.")
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
