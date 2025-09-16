#!/usr/bin/env python3
import argparse, csv, os, sys, time, socket, signal, glob

HOST = socket.gethostname()

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
    "cpu_total_jiffies","mem_free_kb","mem_avail_kb"
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
    # comm can contain spaces inside parentheses; split carefully:
    # format: pid (comm) state ...
    lpar = line.find("(")
    rpar = line.rfind(")")
    comm = line[lpar+1:rpar]
    rest = line[rpar+2:].split()
    # indices per procfs manpage
    state = rest[0]
    ppid = int(rest[1])
    utime = int(rest[11])   # field 14 (0-based after pid/comm), but we used different slicing
    stime = int(rest[12])   # field 15
    nice = int(rest[16])    # field 19
    prio = int(rest[15])    # field 18
    num_threads = int(rest[17]) # field 20
    return comm, state, ppid, utime, stime, nice, prio, num_threads

def read_proc_status_mem(pid):
    s = read_kv_status(f"/proc/{pid}/status")
    # VmSize/VmRSS are in kB already
    vmsize = s.get("VmSize", "0 kB").split()[0]
    vmrss  = s.get("VmRSS",  "0 kB").split()[0]
    vmsize_kb = int(vmsize) if vmsize.isdigit() else 0
    vmrss_kb  = int(vmrss)  if vmrss.isdigit()  else 0
    # context switches
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
                k, v = line.strip().split(":")
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
    # cpu user nice system idle iowait irq softirq steal guest guest_nice
    vals = [int(x) for x in parts[1:]]  # sum them
    return sum(vals)

def read_system_mem():
    mem = {}
    try:
        with open("/proc/meminfo","r") as f:
            for line in f:
                k, v = line.split(":")
                mem[k] = v.strip()
    except:
        pass
    def get_kb(key):
        raw = mem.get(key, "0 kB").split()[0]
        return int(raw) if raw.isdigit() else 0
    return get_kb("MemFree"), get_kb("MemAvailable")

def list_pids_by_grep(substr):
    pids = []
    for path in glob.glob("/proc/[0-9]*/cmdline"):
        pid = path.split("/")[2]
        try:
            with open(path, "rb") as f:
                cmd = f.read().replace(b"\x00", b" ").decode("utf-8", "ignore")
                if substr in cmd:
                    pids.append(int(pid))
        except:
            continue
    return sorted(set(pids))

def trace_loop(pids, interval, out_path):
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(FIELDS)
        while not stop:
            ts_ns = time.time_ns()
            cpu_j = read_system_cpu_jiffies()
            mem_free_kb, mem_avail_kb = read_system_mem()

            # refresh grep-discovered PIDs if requested (pids can be a callable)
            active_pids = pids() if callable(pids) else pids

            for pid in list(active_pids):
                if not os.path.exists(f"/proc/{pid}"):
                    continue
                try:
                    stat = read_proc_stat(pid)
                    if not stat:
                        continue
                    comm, state, ppid, utime, stime, nice, prio, nthreads = stat
                    vmsize_kb, vmrss_kb, vctx, nvctx = read_proc_status_mem(pid)
                    rchar, wchar, rbytes, wbytes, cwb = read_proc_io(pid)

                    row = [
                        ts_ns, HOST, pid, ppid, comm, state,
                        utime, stime, nice, prio, nthreads,
                        vmsize_kb, vmrss_kb,
                        vctx, nvctx,
                        rchar, wchar, rbytes, wbytes, cwb,
                        cpu_j, mem_free_kb, mem_avail_kb
                    ]
                    w.writerow(row)
                except Exception:
                    # process may exit or files become unreadable mid-iteration
                    continue
            f.flush()
            time.sleep(interval)

def main():
    ap = argparse.ArgumentParser(description="Lightweight /proc trace collector")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--pid", type=int, help="PID to trace")
    g.add_argument("--grep", type=str, help="Substring to match in /proc/*/cmdline to trace multiple PIDs")
    ap.add_argument("--interval", type=float, default=0.25, help="Polling interval in seconds (e.g., 0.1)")
    ap.add_argument("--out", type=str, default="proc_trace.csv", help="Output CSV path")
    args = ap.parse_args()

    if args.pid:
        pids = [args.pid]
    else:
        # discover repeatedly so new executors are picked up
        def discover():
            return list_pids_by_grep(args.grep)
        pids = discover

    print(f"[trace] writing to {args.out} (interval={args.interval}s). Press Ctrl+C to stop.", file=sys.stderr)
    trace_loop(pids, args.interval, args.out)

if __name__ == "__main__":
    main()
