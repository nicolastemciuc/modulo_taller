#!/usr/bin/env python3
import argparse, csv, os, sys, time, socket, signal, glob, shutil
from pathlib import Path

HOST = socket.gethostname()
SELF = os.getpid()
DEFAULT_PID_FILE = "/mnt/extradisk/workloads/latest/pids.txt"

SYSTEM_FEATURES = ["memavailable_bytes"]
PID_FEATURES = [
    "disk_read_bytes",
    "disk_write_bytes",
    "vmsize_bytes",
    "stime_ticks",
    "vmdata_bytes",
    "vmrss_bytes",
    "utime_ticks",
]

stop = False
def handle_sigint(signum, frame):
    global stop
    stop = True

signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)

# ---------- /proc helpers ----------
def read_first_line(path):
    try:
        with open(path, "r") as f:
            return f.readline().strip()
    except:
        return None

def read_kv_status(path):
    d = {}
    try:
        with open(path, "r") as f:
            for line in f:
                if ":" in line:
                    k, v = line.split(":", 1)
                    d[k.strip()] = v.strip()
    except:
        pass
    return d

def read_proc_stat(pid):
    line = read_first_line(f"/proc/{pid}/stat")
    if not line: return None
    rpar = line.rfind(")")
    rest = line[rpar+2:].split()
    utime = int(rest[11])
    stime = int(rest[12])
    return utime, stime

def read_proc_status_mem_bytes(pid):
    s = read_kv_status(f"/proc/{pid}/status")
    def kb_to_bytes(field, default="0 kB"):
        raw = s.get(field, default).split()[0]
        return (int(raw) if raw.isdigit() else 0) * 1024
    return (
        kb_to_bytes("VmSize"),
        kb_to_bytes("VmRSS"),
        kb_to_bytes("VmData"),
    )

def read_proc_io(pid):
    rbytes = wbytes = 0
    try:
        with open(f"/proc/{pid}/io","r") as f:
            for line in f:
                if ":" not in line: continue
                k, v = line.strip().split(":", 1)
                v = v.strip()
                if k == "read_bytes": rbytes = int(v)
                elif k == "write_bytes": wbytes = int(v)
    except:
        pass
    return rbytes, wbytes

def read_system_memavailable_bytes():
    mem = {}
    try:
        with open("/proc/meminfo","r") as f:
            for line in f:
                if ":" in line:
                    k, v = line.split(":",1)
                    mem[k] = v.strip()
    except:
        pass
    raw = mem.get("MemAvailable", "0 kB").split()[0]
    kb = int(raw) if raw.isdigit() else 0
    return kb * 1024

# ---------- PID helpers ----------
def list_pids_by_grep(substr):
    pids = []
    for path in glob.glob("/proc/[0-9]*/cmdline"):
        pid = int(path.split("/")[2])
        if pid == SELF:
            continue
        try:
            with open(path, "rb") as f:
                cmd = f.read().replace(b"\x00", b" ").decode("utf-8", "ignore")
                if substr in cmd:
                    pids.append(pid)
        except:
            continue
    return sorted(set(pids))

def read_pid_file(path: str) -> set[int]:
    p = Path(path)
    if not p.exists():
        return set()
    out = set()
    for line in p.read_text().splitlines():
        s = line.strip()
        if not s or s.startswith("#"): continue
        if s.isdigit():
            out.add(int(s))
    return out

def read_pid_dir(dir_path: str) -> set[int]:
    out = set()
    for file in sorted(Path(dir_path).glob("*.pid")):
        out |= read_pid_file(str(file))
    return out

def read_pid_glob(pattern: str) -> set[int]:
    out = set()
    for file in sorted(glob.glob(pattern)):
        out |= read_pid_file(file)
    return out

# ---------- Writers ----------
class FeatureWriters:
    def __init__(self, out_dir: Path, per_pid: bool):
        self.out_dir = out_dir
        self.per_pid = per_pid
        self.writers = {}
        self.pid_writers = {}
        # Ensure clean start
        if self.out_dir.exists():
            shutil.rmtree(self.out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        # Create memavailable.csv (always present)
        self._open_feature_file("memavailable_bytes")

    def _open_feature_file(self, feature: str):
        path = self.out_dir / f"{feature}.csv"
        f = open(path, "w", newline="")
        w = csv.writer(f)
        w.writerow(["ts_ns", "value"])
        self.writers[feature] = (w, f)

    def _open_pid_feature_file(self, feature: str, pid: int):
        key = (feature, pid)
        if key in self.pid_writers:
            return
        path = self.out_dir / f"{feature}.pid{pid}.csv"
        f = open(path, "w", newline="")
        w = csv.writer(f)
        w.writerow(["ts_ns", "value"])
        self.pid_writers[key] = (w, f)

    def write_system(self, ts_ns: int, value: int):
        w, _ = self.writers["memavailable_bytes"]
        w.writerow([ts_ns, value])

    def write_aggregate(self, ts_ns: int, agg: dict):
        for feat, val in agg.items():
            if feat not in self.writers:
                self._open_feature_file(feat)
            w, _ = self.writers[feat]
            w.writerow([ts_ns, val])

    def write_per_pid(self, ts_ns: int, pid: int, per_pid_vals: dict):
        for feat, val in per_pid_vals.items():
            self._open_pid_feature_file(feat, pid)
            w, _ = self.pid_writers[(feat, pid)]
            w.writerow([ts_ns, val])

    def flush(self):
        for _, (_, f) in self.writers.items():
            f.flush()
        for _, (_, f) in self.pid_writers.items():
            f.flush()

    def close(self):
        for _, (_, f) in self.writers.items():
            f.close()
        for _, (_, f) in self.pid_writers.items():
            f.close()

# ---------- Main trace loop ----------
def trace_loop(dynamic_sources, interval, out_dir: Path, per_pid: bool, debug=False):
    src_funcs = []
    for src in dynamic_sources:
        if callable(src):
            src_funcs.append(src)
        else:
            fixed = set(int(x) for x in src)
            src_funcs.append(lambda fixed=fixed: fixed)

    seen = set()
    writers = FeatureWriters(out_dir, per_pid)

    try:
        while not stop:
            ts_ns = time.time_ns()
            memavailable_b = read_system_memavailable_bytes()

            current = set()
            for fn in src_funcs:
                try:
                    current |= set(fn())
                except Exception:
                    continue

            new_pids = [p for p in current if p not in seen]
            if new_pids:
                print(f"[trace] detected new PIDs: {','.join(str(p) for p in sorted(new_pids))}", file=sys.stderr)
                seen |= set(new_pids)

            agg = {feat: 0 for feat in PID_FEATURES}

            for pid in sorted(current):
                if pid == SELF or not os.path.exists(f"/proc/{pid}"):
                    continue
                try:
                    utime, stime = read_proc_stat(pid) or (0, 0)
                    vmsize_b, vmrss_b, vmdata_b = read_proc_status_mem_bytes(pid)
                    rbytes, wbytes = read_proc_io(pid)

                    per_pid_vals = {
                        "disk_read_bytes": rbytes,
                        "disk_write_bytes": wbytes,
                        "vmsize_bytes": vmsize_b,
                        "stime_ticks": stime,
                        "vmdata_bytes": vmdata_b,
                        "vmrss_bytes": vmrss_b,
                        "utime_ticks": utime,
                    }

                    for k, v in per_pid_vals.items():
                        agg[k] += v

                    if per_pid:
                        writers.write_per_pid(ts_ns, pid, per_pid_vals)

                    if debug:
                        print(f"[PID {pid}] " + ", ".join(f"{k}={v}" for k,v in per_pid_vals.items()))
                except Exception:
                    continue

            writers.write_system(ts_ns, memavailable_b)
            writers.write_aggregate(ts_ns, agg)
            writers.flush()
            time.sleep(interval)
    finally:
        writers.close()

def main():
    ap = argparse.ArgumentParser(description="Writes one CSV per feature (overrides old files each run).")
    ap.add_argument("--pid", type=int, action="append", help="PID to trace")
    ap.add_argument("--grep", type=str, help="Match substring in /proc/*/cmdline")
    ap.add_argument("--pid-file", type=str, default=DEFAULT_PID_FILE,
                    help=f"PID file (default: {DEFAULT_PID_FILE})")
    ap.add_argument("--pid-dir", type=str, help="Directory of *.pid files")
    ap.add_argument("--pid-glob", type=str, help="Glob for *.pid files")
    ap.add_argument("--interval", type=float, default=0.25, help="Polling interval in seconds")
    ap.add_argument("--out-dir", type=str, default="polling_traces",
                    help="Output directory (default: ./polling_traces)")
    ap.add_argument("--per-pid", action="store_true", help="Write one file per PID for PID features")
    ap.add_argument("-d","--debug", action="store_true", help="Print each row")
    args = ap.parse_args()

    sources = [lambda: read_pid_file(args.pid_file)]
    if args.pid_dir:
        sources.append(lambda: read_pid_dir(args.pid_dir))
    if args.pid_glob:
        sources.append(lambda: read_pid_glob(args.pid_glob))
    if args.grep:
        sources.append(lambda: set(list_pids_by_grep(args.grep)))
    if args.pid:
        sources.append(set(args.pid))

    out_dir = Path(args.out_dir)
    print(f"[trace] Writing fresh CSVs to {out_dir} (per_pid={args.per_pid}, interval={args.interval}s)", file=sys.stderr)
    trace_loop(sources, args.interval, out_dir, per_pid=args.per_pid, debug=args.debug)

if __name__ == "__main__":
    main()
