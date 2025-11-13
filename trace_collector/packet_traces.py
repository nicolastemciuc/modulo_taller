#!/usr/bin/env python3
import csv, re, subprocess
from pathlib import Path

TCPDUMP_CMD = "tcpdump_pfring"
LINE_RE = re.compile(
    r'^(?P<ts_epoch>\d+\.\d+)\s+IP\s+'
    r'(?P<src_ip>\d+\.\d+\.\d+\.\d+)\.(?P<src_port>\d+)\s+>\s+'
    r'(?P<dst_ip>\d+\.\d+\.\d+\.\d+)\.(?P<dst_port>\d+):.*length\s+(?P<length>\d+)'
)
DEFAULT_FILTER = "not port 22 and ip and tcp and ((ip[2:2]-((ip[0]&0x0f)<<2)-((tcp[12]&0xf0)>>2))>0)"

# Repo-relative output path
REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = REPO_ROOT / "trace_collector" / "packet_traces.csv"

def main():
    cmd = ["sudo", TCPDUMP_CMD, "-tt", "-ni", "ens3", "-l", DEFAULT_FILTER]

    # Ensure parent dir exists (it should, but just in case)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(OUT_PATH, "w", newline="", encoding="utf-8") as csv_f:
        w = csv.writer(csv_f)
        w.writerow([
            "ts_ns",
            "timestamp_epoch_s",
            "source_ip",
            "source_port",
            "destination_ip",
            "destination_port",
            "packet_size",
        ])

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
                w.writerow([
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
            pass
        finally:
            try:
                proc.terminate()
                proc.wait(timeout=3)
            except Exception:
                proc.kill()

if __name__ == "__main__":
    main()
