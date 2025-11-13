#!/usr/bin/env python3
import csv, re, subprocess, configparser, sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "trace_collector" / "interface.conf"

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

INTERFACE = config["network"].get("interface")
TCPDUMP_CMD = config["collector"].get("tcpdump_cmd", "tcpdump")
DEFAULT_FILTER = config["collector"].get("filter", "ip")

if not (INTERFACE and TCPDUMP_CMD and DEFAULT_FILTER):
    print("ERROR: Missing configuration values:")
    sys.exit(1)

LINE_RE = re.compile(
    r'^(?P<ts_epoch>\d+\.\d+)\s+IP\s+'
    r'(?P<src_ip>\d+\.\d+\.\d+\.\d+)\.(?P<src_port>\d+)\s+>\s+'
    r'(?P<dst_ip>\d+\.\d+\.\d+\.\d+)\.(?P<dst_port>\d+):.*length\s+(?P<length>\d+)'
)

def main():
    cmd = ["sudo", TCPDUMP_CMD, "-tt", "-ni", INTERFACE, "-l", DEFAULT_FILTER]
    with open("packet_traces.csv", "w", newline="", encoding="utf-8") as csv_f:
        w = csv.writer(csv_f)
        w.writerow(["ts_ns","timestamp_epoch_s","source_ip","source_port","destination_ip","destination_port","packet_size"])
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        try:
            assert proc.stdout is not None
            for line in proc.stdout:
                m = LINE_RE.search(line)
                if not m:
                    continue
                ts_epoch = float(m.group("ts_epoch"))
                ts_ns = int(ts_epoch * 1_000_000_000)
                w.writerow([ts_ns, f"{ts_epoch:.6f}", m.group("src_ip"), m.group("src_port"), m.group("dst_ip"), m.group("dst_port"), m.group("length")])
                csv_f.flush()
        except KeyboardInterrupt:
            pass
        finally:
            try:
                proc.terminate(); proc.wait(timeout=3)
            except Exception:
                proc.kill()

if __name__ == "__main__":
    main()
