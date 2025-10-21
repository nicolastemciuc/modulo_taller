#!/usr/bin/env python3
import argparse, csv, os, sys, time, socket, signal, glob, re
from pathlib import Path

HOST = socket.gethostname()
SELF = os.getpid()

DEFAULT_PID_FILE = "/mnt/extradisk/workloads/latest/pids.txt"

# Output schema (same order as before)
FIELDS = [
    "ts_ns",
    "host",
    "pid",
    "disk_read_bytes",
    "disk_write_bytes",
    "vmsize_bytes",
    "memavailable_bytes",
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
    lpar = line.find("("); rpar = line.rfind(")")
    rest = line[rpar+2:].split()
    state = rest[0]
    utime = int(rest[11])
    stime = int(rest[12])
    return state, utime, stime

def read_proc_status_mem_bytes(pid):
    """
    Returns (vmsize_bytes, vmrss_bytes, vmdata_bytes) from /proc/<pid>/status.
    Any missing value -> 0.
    """
    s = read_kv_status(f"/proc/{pid}/status")
    def kb_to_bytes(field, default="0 kB"):
        raw = s.get(field, default).split()[0]
        return (int(raw) if raw.isdigit() else 0) * 1024
    vmsize_b = kb_to_bytes("VmSize")
    vmrss_b  = kb_to_bytes("VmRSS")
    vmdata_b = kb_to_bytes("VmData")
    return vmsize_b, vmrss_b, vmdata_b

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
    """Read newline-separated PIDs; ignore blanks/comments."""
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
    """Read all *.pid files in a directory."""
    out = set()
    for file in sorted(Path(dir_path).glob("*.pid")):
        out |= read_pid_file(str(file))
    return out

def read_pid_glob(pattern: str) -> set[int]:
    """Read all matching *.pid files from a glob pattern."""
    out = set()
    for file in sorted(glob.glob(pattern)):
        out |= read_pid_file(file)
    return out

def trace_loop(dynamic_sources, interval, out_path, debug=False):
    # Normalize sources to callables
    src_funcs = []
    for src in dynamic_sources:
        if callable(src):
            src_funcs.append(src)
        else:
            fixed = set(int(x) for x in src)
            src_funcs.append(lambda fixed=fixed: fixed)

    seen = set()

    # Overwrite each run
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(FIELDS)
        while not stop:
            ts_ns = time.time_ns()
            memavailable_b = read_system_memavailable_bytes()

            # Union of all PID sources (dynamic + static)
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

            for pid in sorted(current):
                if pid == SELF or not os.path.exists(f"/proc/{pid}"):
                    continue
                try:
                    stat = read_proc_stat(pid)
                    if not stat:
                        continue
                    state, utime, stime = stat
                    vmsize_b, vmrss_b, vmdata_b = read_proc_status_mem_bytes(pid)
                    rbytes, wbytes = read_proc_io(pid)

                    row = [
                        ts_ns, HOST, pid,
                        rbytes, wbytes,
                        vmsize_b,
                        memavailable_b,
                        stime,
                        vmdata_b,
                        vmrss_b,
                        utime,
                    ]
                    w.writerow(row)
                    if debug:
                        print(",".join(str(x) for x in row))
                except Exception:
                    continue
            f.flush()
            time.sleep(interval)

def main():
    ap = argparse.ArgumentParser(description="Minimal /proc trace collector for selected metrics")
    ap.add_argument("--pid", type=int, action="append",
                    help="PID to trace (can be specified multiple times)")
    ap.add_argument("--grep", type=str,
                    help="Substring to match in /proc/*/cmdline to trace multiple PIDs (dynamic)")
    ap.add_argument("--pid-file", type=str, default=DEFAULT_PID_FILE,
                    help=f"Path to newline-separated PID file (default: {DEFAULT_PID_FILE})")
    ap.add_argument("--pid-dir", type=str,
                    help="Directory containing one or more *.pid files (each file may hold multiple PIDs)")
    ap.add_argument("--pid-glob", type=str,
                    help="Glob for *.pid files (e.g., '/mnt/extradisk/workloads/latest/*.pid')")
    ap.add_argument("--interval", type=float, default=0.25,
                    help="Polling interval in seconds (e.g., 0.1)")
    ap.add_argument("--out", type=str, default="polling_traces.csv",
                    help="Output CSV path (default: polling_traces.csv)")
    ap.add_argument("-d","--debug", action="store_true",
                    help="Also print each collected row to stdout")
    args = ap.parse_args()

    sources = []

    # 1) PID file (dynamic)
    def from_pid_file():
        return read_pid_file(args.pid_file)
    sources.append(from_pid_file)

    # 2) PID dir (dynamic)
    if args.pid_dir:
        def from_pid_dir():
            return read_pid_dir(args.pid_dir)
        sources.append(from_pid_dir)

    # 3) PID glob (dynamic)
    if args.pid_glob:
        def from_pid_glob():
            return read_pid_glob(args.pid_glob)
        sources.append(from_pid_glob)

    # 4) Grep discovery (dynamic)
    if args.grep:
        def from_grep():
            return set(list_pids_by_grep(args.grep))
        sources.append(from_grep)

    # 5) Explicit PIDs (static)
    if args.pid:
        sources.append(set(args.pid))

    print(
        f"[trace] writing to {args.out} (interval={args.interval}s). "
        f"watching pid_file={args.pid_file}"
        f"{' pid_dir='+args.pid_dir if args.pid_dir else ''}"
        f"{' pid_glob='+args.pid_glob if args.pid_glob else ''}"
        f"{' grep='+args.grep if args.grep else ''}"
        f"{' pids='+','.join(map(str,args.pid)) if args.pid else ''}",
        file=sys.stderr
    )

    trace_loop(sources, args.interval, args.out, debug=args.debug)

if __name__ == "__main__":
    main()
