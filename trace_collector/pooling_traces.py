#!/usr/bin/env python3
import argparse, csv, os, sys, time, socket, signal, glob, re

HOST = socket.gethostname()
SELF = os.getpid()

FIELDS = [
    # timing & identity
    "ts_ns","host","pid","ppid","comm","state",
    # CPU time (process)
    "utime_ticks","stime_ticks","nice","prio","num_threads",
    # memory (process)
    "vmsize_kb","vmrss_kb",
    # context switches
    "voluntary_ctxt","nonvoluntary_ctxt",
    # IO (process)
    "rchar","wchar","read_bytes","write_bytes","cancelled_write_bytes",
    # system snapshot (for normalization)
    "cpu_total_jiffies","mem_free_kb","mem_avail_kb",
    # spark-ish tags parsed from cmdline
    "spark_class","spark_executor_id","jar",
    # PSI (pressure stall info)
    "psi_cpu_some_avg10","psi_cpu_some_avg60","psi_cpu_some_avg300",
    "psi_mem_some_avg10","psi_mem_some_avg60","psi_mem_some_avg300",
    "psi_mem_full_avg10","psi_mem_full_avg60","psi_mem_full_avg300",
    "psi_io_some_avg10","psi_io_some_avg60","psi_io_some_avg300",
    "psi_io_full_avg10","psi_io_full_avg60","psi_io_full_avg300",
    # diskstats aggregate (system)
    "disk_read_ios","disk_read_sectors","disk_write_ios","disk_write_sectors",
    "disk_inflight","disk_io_ticks"
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
    comm = line[lpar+1:rpar]
    rest = line[rpar+2:].split()
    state = rest[0]
    ppid = int(rest[1])
    utime = int(rest[11])
    stime = int(rest[12])
    nice = int(rest[16])
    prio = int(rest[15])
    num_threads = int(rest[17])
    return comm, state, ppid, utime, stime, nice, prio, num_threads

def read_proc_status_mem(pid):
    s = read_kv_status(f"/proc/{pid}/status")
    vmsize = s.get("VmSize", "0 kB").split()[0]
    vmrss  = s.get("VmRSS",  "0 kB").split()[0]
    vmsize_kb = int(vmsize) if vmsize.isdigit() else 0
    vmrss_kb  = int(vmrss)  if vmrss.isdigit()  else 0
    vctx = s.get("voluntary_ctxt_switches", "0").split()[0]
    nvctx = s.get("nonvoluntary_ctxt_switches", "0").split()[0]
    voluntary = int(vctx) if vctx.isdigit() else 0
    nonvol    = int(nvctx) if nvctx.isdigit() else 0
    return vmsize_kb, vmrss_kb, voluntary, nonvol

def read_proc_io(pid):
    rchar = wchar = rbytes = wbytes = cwb = 0
    try:
        with open(f"/proc/{pid}/io","r") as f:
            for line in f:
                if ":" not in line: continue
                k, v = line.strip().split(":", 1)
                v = v.strip()
                if k == "rchar": rchar = int(v)
                elif k == "wchar": wchar = int(v)
                elif k == "read_bytes": rbytes = int(v)
                elif k == "write_bytes": wbytes = int(v)
                elif k == "cancelled_write_bytes": cwb = int(v)
    except:
        pass
    return rchar, wchar, rbytes, wbytes, cwb

def read_system_cpu_jiffies():
    line = read_first_line("/proc/stat")
    if not line or not line.startswith("cpu "):
        return 0
    parts = line.split()
    vals = [int(x) for x in parts[1:]]
    return sum(vals)

def read_system_mem():
    mem = {}
    try:
        with open("/proc/meminfo","r") as f:
            for line in f:
                if ":" in line:
                    k, v = line.split(":",1)
                    mem[k] = v.strip()
    except:
        pass
    def get_kb(key):
        raw = mem.get(key, "0 kB").split()[0]
        return int(raw) if raw.isdigit() else 0
    return get_kb("MemFree"), get_kb("MemAvailable")

def parse_pressure_line(s):
    vals = {"avg10":0.0,"avg60":0.0,"avg300":0.0}
    if not s: return vals
    for part in s.split():
        if part.startswith("avg10="):  vals["avg10"]  = float(part.split("=")[1])
        elif part.startswith("avg60="): vals["avg60"]  = float(part.split("=")[1])
        elif part.startswith("avg300="):vals["avg300"] = float(part.split("=")[1])
    return vals

def read_psi():
    def read(path):
        try:
            with open(path,"r") as f:
                lines = [ln.strip() for ln in f.readlines()]
                return {ln.split()[0]: ln for ln in lines if ln}
        except:
            return {}
    cpu = read("/proc/pressure/cpu")     # only "some"
    mem = read("/proc/pressure/memory")  # "some" and "full"
    io  = read("/proc/pressure/io")      # "some" and "full"
    cpu_some = parse_pressure_line(cpu.get("some",""))
    mem_some = parse_pressure_line(mem.get("some",""))
    mem_full = parse_pressure_line(mem.get("full",""))
    io_some  = parse_pressure_line(io.get("some",""))
    io_full  = parse_pressure_line(io.get("full",""))
    return (
        cpu_some["avg10"], cpu_some["avg60"], cpu_some["avg300"],
        mem_some["avg10"], mem_some["avg60"], mem_some["avg300"],
        mem_full["avg10"], mem_full["avg60"], mem_full["avg300"],
        io_some["avg10"], io_some["avg60"], io_some["avg300"],
        io_full["avg10"], io_full["avg60"], io_full["avg300"]
    )

# Aggregate /proc/diskstats across non-virtual, non-partition devices
DEV_SKIP_PREFIXES = ("loop","ram","md","dm-","zd","sr")
def read_diskstats():
    rd_ios=rd_sec=wr_ios=wr_sec=inflight=io_ticks=0
    try:
        with open("/proc/diskstats","r") as f:
            for line in f:
                parts = line.split()
                if len(parts) < 14:
                    continue
                name = parts[2]
                if any(name.startswith(p) for p in DEV_SKIP_PREFIXES): 
                    continue
                if re.search(r".*\d+$", name):  # skip partitions (sda1, nvme0n1p2, etc.)
                    continue
                try:
                    rd_ios   += int(parts[3])
                    rd_sec   += int(parts[5])
                    wr_ios   += int(parts[7])
                    wr_sec   += int(parts[9])
                    inflight += int(parts[11])
                    io_ticks += int(parts[12])
                except:
                    continue
    except:
        pass
    return rd_ios, rd_sec, wr_ios, wr_sec, inflight, io_ticks

def read_cmdline(pid):
    try:
        with open(f"/proc/{pid}/cmdline","rb") as f:
            return f.read().split(b"\x00")
    except:
        return []

def parse_spark_tags(pid):
    args = [a.decode("utf-8","ignore") for a in read_cmdline(pid) if a]
    spark_class = None
    jar = None
    executor_id = None
    for i, a in enumerate(args):
        if a == "--class" and i+1 < len(args):
            spark_class = args[i+1]
        if a.endswith(".jar") and jar is None:
            jar = os.path.basename(a)
        if a == "-executor-id" and i+1 < len(args):
            executor_id = args[i+1]
    return spark_class or "", executor_id or "", jar or ""

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

def trace_loop(pids, interval, out_path, debug=False):
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(FIELDS)
        while not stop:
            ts_ns = time.time_ns()
            cpu_j = read_system_cpu_jiffies()
            mem_free_kb, mem_avail_kb = read_system_mem()
            psi_vals = read_psi()
            disk_vals = read_diskstats()

            active_pids = pids() if callable(pids) else pids

            for pid in list(active_pids):
                if pid == SELF or not os.path.exists(f"/proc/{pid}"):
                    continue
                try:
                    stat = read_proc_stat(pid)
                    if not stat:
                        continue
                    comm, state, ppid, utime, stime, nice, prio, nthreads = stat
                    vmsize_kb, vmrss_kb, vctx, nvctx = read_proc_status_mem(pid)
                    rchar, wchar, rbytes, wbytes, cwb = read_proc_io(pid)
                    spark_class, executor_id, jar = parse_spark_tags(pid)

                    row = [
                        ts_ns, HOST, pid, ppid, comm, state,
                        utime, stime, nice, prio, nthreads,
                        vmsize_kb, vmrss_kb,
                        vctx, nvctx,
                        rchar, wchar, rbytes, wbytes, cwb,
                        cpu_j, mem_free_kb, mem_avail_kb,
                        spark_class, executor_id, jar,
                        *psi_vals,
                        *disk_vals
                    ]
                    w.writerow(row)
                    if debug:
                        print(",".join(str(x) for x in row))
                except Exception:
                    continue
            f.flush()
            time.sleep(interval)

def main():
    ap = argparse.ArgumentParser(description="Lightweight /proc trace collector (+PSI, diskstats, Spark tags)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--pid", type=int, help="PID to trace")
    g.add_argument("--grep", type=str, help="Substring to match in /proc/*/cmdline to trace multiple PIDs")
    ap.add_argument("--interval", type=float, default=0.25, help="Polling interval in seconds (e.g., 0.1)")
    ap.add_argument("--out", type=str, default="proc_trace.csv", help="Output CSV path")
    ap.add_argument("-d","--debug", action="store_true", help="Also print each collected row to stdout")
    args = ap.parse_args()

    if args.pid:
        pids = [args.pid]
    else:
        def discover():
            return list_pids_by_grep(args.grep)
        pids = discover

    print(f"[trace] writing to {args.out} (interval={args.interval}s). Press Ctrl+C to stop.", file=sys.stderr)
    trace_loop(pids, args.interval, args.out, debug=args.debug)

if __name__ == "__main__":
    main()
