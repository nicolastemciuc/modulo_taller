#!/usr/bin/env python3
import csv, os, time, signal, shutil
from pathlib import Path

SELF = os.getpid()
PID_FILE = "../workloads/latest/pids.txt"
OUT_DIR = Path("polling_traces")
INTERVAL = 0.25

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
def _stop(*_):  # SIGINT/SIGTERM handler
    global stop
    stop = True

signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)

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
    if not line:
        return None
    rpar = line.rfind(")")
    rest = line[rpar + 2 :].split()
    return int(rest[11]), int(rest[12])  # utime, stime

def read_proc_status_mem_bytes(pid):
    s = read_kv_status(f"/proc/{pid}/status")

    def kb(field):
        raw = (s.get(field, "0 kB").split()[0])
        return (int(raw) if raw.isdigit() else 0) * 1024

    return kb("VmSize"), kb("VmRSS"), kb("VmData")

def read_proc_io(pid):
    rbytes = wbytes = 0
    try:
        with open(f"/proc/{pid}/io", "r") as f:
            for line in f:
                if ":" not in line:
                    continue
                k, v = line.strip().split(":", 1)
                v = v.strip()
                if k == "read_bytes":
                    rbytes = int(v)
                elif k == "write_bytes":
                    wbytes = int(v)
    except:
        pass
    return rbytes, wbytes

def read_system_memavailable_bytes():
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    val = line.split(":", 1)[1].strip().split()[0]
                    return (int(val) if val.isdigit() else 0) * 1024
    except:
        pass
    return 0

def read_pid_file(path: str) -> set[int]:
    p = Path(path)
    if not p.exists():
        return set()
    out = set()
    for line in p.read_text().splitlines():
        s = line.strip()
        if s.isdigit():
            out.add(int(s))
    return out

class Writers:
    def __init__(self, out_dir: Path):
        if out_dir.exists():
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        self.out_dir = out_dir
        self.sys_w = self._open("memavailable_bytes.csv")
        self.pid_writers = {}

    def _open(self, name):
        f = open(self.out_dir / name, "w", newline="")
        w = csv.writer(f)
        w.writerow(["ts_ns", "value"])
        return (w, f)

    def write_pid_metric(self, ts_ns, pid, metric, value):
        key = (metric, pid)
        if key not in self.pid_writers:
            filename = f"{metric}_{pid}.csv"
            self.pid_writers[key] = self._open(filename)
        w, _ = self.pid_writers[key]
        w.writerow([ts_ns, value])

    def write_system(self, ts_ns, val):
        w, _ = self.sys_w
        w.writerow([ts_ns, val])

    def flush(self):
        self.sys_w[1].flush()
        for (_, f) in self.pid_writers.values():
            f.flush()

    def close(self):
        self.sys_w[1].close()
        for (_, f) in self.pid_writers.values():
            f.close()

def main():
    writers = Writers(OUT_DIR)
    try:
        while not stop:
            ts_ns = time.time_ns()
            memavailable_b = read_system_memavailable_bytes()

            for pid in sorted(read_pid_file(PID_FILE)):
                if pid == SELF or not os.path.exists(f"/proc/{pid}"):
                    continue
                try:
                    utime, stime = read_proc_stat(pid) or (0, 0)
                    vmsize_b, vmrss_b, vmdata_b = read_proc_status_mem_bytes(pid)
                    rbytes, wbytes = read_proc_io(pid)

                    writers.write_pid_metric(ts_ns, pid, "disk_read_bytes",  rbytes)
                    writers.write_pid_metric(ts_ns, pid, "disk_write_bytes", wbytes)
                    writers.write_pid_metric(ts_ns, pid, "vmsize_bytes",     vmsize_b)
                    writers.write_pid_metric(ts_ns, pid, "vmrss_bytes",      vmrss_b)
                    writers.write_pid_metric(ts_ns, pid, "vmdata_bytes",     vmdata_b)
                    writers.write_pid_metric(ts_ns, pid, "utime_ticks",      utime)
                    writers.write_pid_metric(ts_ns, pid, "stime_ticks",      stime)
                except:
                    continue

            writers.write_system(ts_ns, memavailable_b)
            writers.flush()
            time.sleep(INTERVAL)
    finally:
        writers.close()

if __name__ == "__main__":
    main()
