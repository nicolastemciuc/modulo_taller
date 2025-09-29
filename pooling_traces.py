#!/usr/bin/env python3
import argparse, csv, os, sys, time, socket, signal, glob, re, struct
from typing import List, Dict, Tuple

HOST = socket.gethostname()
SELF = os.getpid()

# -----------------------------
# Output fields (original + 6-tuple + extras)
# -----------------------------
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
    "disk_inflight","disk_io_ticks",
    # === socket attribution (per-row, one per socket) ===
    "proto", "src_ip", "src_port", "dst_ip", "dst_port", "sock_inode", "tcp_state",
    # === flow gap ===
    "flow_gap_ms"
]

# Track last-seen timestamp per 6-tuple key to compute gap
# Key: (proto, src_ip, src_port, dst_ip, dst_port)
_last_seen_ns: Dict[Tuple[str,str,int,str,int], int] = {}

stop = False
def handle_sigint(signum, frame):
    global stop
    stop = True

signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)

# -----------------------------
# /proc helpers
# -----------------------------
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
    pids: List[int] = []
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

# -----------------------------
# Socket table parsing (/proc/<pid>/net/*)
# -----------------------------
_TCP_STATE_MAP = {
    "01":"ESTABLISHED", "02":"SYN_SENT", "03":"SYN_RECV", "04":"FIN_WAIT1",
    "05":"FIN_WAIT2", "06":"TIME_WAIT", "07":"CLOSE", "08":"CLOSE_WAIT",
    "09":"LAST_ACK", "0A":"LISTEN", "0B":"CLOSING", "0C":"NEW_SYN_RECV"
}

def _hex_to_ipv4(h: str) -> str:
    # /proc/net/tcp uses little-endian for the 32-bit address field
    # h example: '0100007F' -> 127.0.0.1
    try:
        n = int(h, 16)
        return socket.inet_ntoa(struct.pack("<I", n))
    except Exception:
        return "0.0.0.0"

def _swap_32le_words(b: bytes) -> bytes:
    # swap endianness per 32-bit word (tcp6 uses LE words)
    if len(b) != 16: return b
    out = bytearray()
    for i in range(0,16,4):
        out += b[i:i+4][::-1]
    return bytes(out)

def _hex_to_ipv6(h: str) -> str:
    # /proc/net/tcp6: 32 hex chars (16 bytes) in LE 32-bit words
    try:
        raw = bytes.fromhex(h)
        be = _swap_32le_words(raw)
        return socket.inet_ntop(socket.AF_INET6, be)
    except Exception:
        return "::"

def _parse_ipport(ip_hex: str, port_hex: str, ipv6: bool=False):
    ip = _hex_to_ipv6(ip_hex) if ipv6 else _hex_to_ipv4(ip_hex)
    port = int(port_hex, 16)
    return ip, port

def _read_pid_sockets(pid: int, proto: str):
    """
    proto in {"tcp","tcp6","udp","udp6"}
    Returns list of dicts with keys:
      proto, ipv, state, inode, l_ip, l_port, r_ip, r_port
    """
    path = f"/proc/{pid}/net/{proto}"
    conns = []
    try:
        with open(path, "r") as f:
            next(f)  # header
            for line in f:
                parts = line.strip().split()
                if len(parts) < 10:
                    continue
                laddr_hex, lport_hex = parts[1].split(":")
                raddr_hex, rport_hex = parts[2].split(":")
                state = parts[3]
                inode = parts[9]
                ipv6 = proto.endswith("6")
                lip, lpt = _parse_ipport(laddr_hex, lport_hex, ipv6)
                rip, rpt = _parse_ipport(raddr_hex, rport_hex, ipv6)
                conns.append({
                    "proto": "tcp" if "tcp" in proto else "udp",
                    "ipv": 6 if ipv6 else 4,
                    "state": state.upper(),
                    "inode": int(inode) if inode.isdigit() else -1,
                    "l_ip": lip, "l_port": lpt,
                    "r_ip": rip, "r_port": rpt,
                })
    except FileNotFoundError:
        pass
    except Exception:
        pass
    return conns

def list_pid_flows(pid: int):
    flows = []
    for proto in ("tcp","tcp6","udp","udp6"):
        flows.extend(_read_pid_sockets(pid, proto))
    return flows

# -----------------------------
# Flow gap computation
# -----------------------------
def compute_flow_gap_ms(ts_ns: int, key: Tuple[str,str,int,str,int]) -> float:
    """Return milliseconds since last time we saw this 6-tuple; 0.0 if first time."""
    prev = _last_seen_ns.get(key)
    _last_seen_ns[key] = ts_ns
    if prev is None:
        return 0.0
    return (ts_ns - prev) / 1e6

# -----------------------------
# Main trace loop
# -----------------------------
def trace_loop(pids, interval, out_path, tcp_established_only=False, emit_row_when_no_flow=True, debug=False):
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

                    base = [
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

                    flows = list_pid_flows(pid)

                    wrote_any = False
                    for c in flows:
                        if c["proto"] == "tcp" and tcp_established_only and c["state"] != "01":
                            # Only include ESTABLISHED if flag is set
                            continue
                        proto_str = f'{c["proto"]}{c["ipv"]}'
                        tup_key = (proto_str, c["l_ip"], c["l_port"], c["r_ip"], c["r_port"])
                        gap_ms = compute_flow_gap_ms(ts_ns, tup_key)
                        row = base + [
                            proto_str,
                            c["l_ip"], c["l_port"], c["r_ip"], c["r_port"],
                            c["inode"],
                            _TCP_STATE_MAP.get(c["state"], c["state"]),
                            f"{gap_ms:.3f}"
                        ]
                        w.writerow(row)
                        wrote_any = True
                        if debug:
                            print(",".join(str(x) for x in row))

                    if not wrote_any and emit_row_when_no_flow:
                        # Keep time series continuity even when the process has no visible sockets
                        row = base + ["", "", "", "", "", "", "", "0.000"]
                        w.writerow(row)
                        if debug:
                            print(",".join(str(x) for x in row))

                except Exception:
                    # Best-effort: skip PID if any read fails mid-interval
                    continue

            f.flush()
            time.sleep(interval)

def main():
    ap = argparse.ArgumentParser(
        description="Lightweight /proc trace collector (+PSI, diskstats, Spark tags) with per-socket 6-tuple attribution"
    )
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--pid", type=int, help="PID to trace")
    g.add_argument("--grep", type=str, help="Substring to match in /proc/*/cmdline to trace multiple PIDs")
    ap.add_argument("--interval", type=float, default=0.25, help="Polling interval in seconds (e.g., 0.1)")
    ap.add_argument("--out", type=str, default="proc_trace.csv", help="Output CSV path")
    ap.add_argument("--tcp-established-only", action="store_true",
                    help="If set, only include TCP sockets in ESTABLISHED state")
    ap.add_argument("--no-empty-rows", action="store_true",
                    help="If set, do not emit a row when a PID has no sockets in the current interval")
    ap.add_argument("-d","--debug", action="store_true", help="Also print each collected row to stdout")
    args = ap.parse_args()

    if args.pid:
        pids = [args.pid]
    else:
        def discover():
            return list_pids_by_grep(args.grep)
        pids = discover

    emit_row_when_no_flow = not args.no_empty_rows

    print(f"[trace] writing to {args.out} (interval={args.interval}s). Press Ctrl+C to stop.", file=sys.stderr)
    trace_loop(
        pids,
        args.interval,
        args.out,
        tcp_established_only=args.tcp_established_only,
        emit_row_when_no_flow=emit_row_when_no_flow,
        debug=args.debug
    )

if __name__ == "__main__":
    main()
