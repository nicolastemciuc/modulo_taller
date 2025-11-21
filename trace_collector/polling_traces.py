#!/usr/bin/env python3
import csv, os, time, signal, shutil, sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from config_loader import CFG

SELF = os.getpid()
PID_FILE = REPO_ROOT / CFG["global"]["workload_pid_file"]
OUT_DIR = REPO_ROOT / CFG["polling_traces"]["output_dir"]
INTERVAL = CFG["polling_traces"]["time_interval"]
PID_FEATURES = CFG["polling_traces"]["features"]

stop = False
def _stop(*_):
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
        raw = s.get(field, "0 kB").split()[0]
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
    """
    - One system-wide file: memavailable_bytes.csv
    - One file per (PID, feature): <feature>_pid<PID>.csv
    """

    def __init__(self, out_dir: Path):
        if out_dir.exists():
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        self.out_dir = out_dir

        # System-wide memavailable
        self.sys_w = self._open_system("memavailable_bytes.csv")

        # (pid, feature) -> (csv.writer, file)
        self.writers = {}

    def _open_system(self, name):
        f = open(self.out_dir / name, "w", newline="")
        w = csv.writer(f)
        w.writerow(["ts_ns", "value"])
        return (w, f)

    def write_system(self, ts_ns, val):
        w, _ = self.sys_w
        w.writerow([ts_ns, val])

    def write_pid_feature(self, ts_ns, pid, feat, val):
        key = (pid, feat)
        if key not in self.writers:
            fname = f"{feat}_pid{pid}.csv"
            f = open(self.out_dir / fname, "w", newline="")
            w = csv.writer(f)
            w.writerow(["ts_ns", "value"])
            self.writers[key] = (w, f)
        w, _ = self.writers[key]
        w.writerow([ts_ns, val])

    def flush(self):
        self.sys_w[1].flush()
        for _, (_, f) in self.writers.items():
            f.flush()

    def close(self):
        self.sys_w[1].close()
        for _, (_, f) in self.writers.items():
            f.close()


def main():
    writers = Writers(OUT_DIR)
    try:
        while not stop:
            ts_ns = time.time_ns()

            # System-wide metric
            memavailable_b = read_system_memavailable_bytes()
            writers.write_system(ts_ns, memavailable_b)

            # Per-PID metrics
            for pid in sorted(read_pid_file(PID_FILE)):
                if pid == SELF or not os.path.exists(f"/proc/{pid}"):
                    continue

                try:
                    utime, stime = read_proc_stat(pid) or (0, 0)
                    vmsize_b, vmrss_b, vmdata_b = read_proc_status_mem_bytes(pid)
                    rbytes, wbytes = read_proc_io(pid)

                    values = {
                        "disk_read_bytes": rbytes,
                        "disk_write_bytes": wbytes,
                        "vmsize_bytes": vmsize_b,
                        "stime_ticks": stime,
                        "vmdata_bytes": vmdata_b,
                        "vmrss_bytes": vmrss_b,
                        "utime_ticks": utime,
                    }

                    # Respect the configured list of features
                    for feat in PID_FEATURES:
                        if feat in values:
                            writers.write_pid_feature(ts_ns, pid, feat, values[feat])
                except Exception:
                    continue

            writers.flush()
            time.sleep(INTERVAL)
    finally:
        writers.close()


if __name__ == "__main__":
    main()
