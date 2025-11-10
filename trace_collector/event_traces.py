#!/usr/bin/env python3
import argparse, os, sys, signal, subprocess, time

SELF = os.getpid()
SCRIPT_DIR = "./event_scripts/"
RECORD_DIR = "./event_traces/"
RATE_NS = "500000"
DEFAULT_PID_FILE = "/mnt/extradisk/workloads/latest/pids.txt"

SCRIPTS = {
    "cuda_mem.py":   {"args": ["-s", RATE_NS], "record": "cuda_allocations"},
    "cpu_mem.py":    {"args": ["-s", RATE_NS], "record": "cpu_allocations"},
    "nccl_mem.py":   {"args": ["-s", RATE_NS], "record": "cuda_collective"},
    "kcache.py":     {"args": [],              "record": "kcache"},
    "sendmsg.py":    {"args": [],              "record": "sendmsg"},
    "sendto.py":     {"args": [],              "record": "sendto"},
    "tcpsendmsg.py": {"args": [],              "record": "tcpsendmsg"},
    "write.py":      {"args": [],              "record": "write"},
}

stop = False

def handle_sigint(signum, frame):
    global stop
    stop = True

signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)

def create_log_dir(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def launch_script(script, pid_str, cfg):
    """
    Launch one script from event_scripts/ with its own csv log.
    """
    script_path = os.path.join(SCRIPT_DIR, script)
    if not os.path.exists(script_path):
        print(f"[warn] script not found: {script_path}", file=sys.stderr)
        return None, None

    log_path = os.path.join(RECORD_DIR, f"{cfg['record']}_pid{pid_str}.csv")
    create_log_dir(log_path)

    cmd = ["sudo", sys.executable, script_path, "-p", pid_str] + cfg["args"]
    try:
        log_file = open(log_path, "w", newline="")
        proc = subprocess.Popen(
            cmd,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid
        )
        return proc, log_file
    except Exception as e:
        print(f"[error] failed to launch {script}: {e}", file=sys.stderr)
        return None, None

def stop_all(processes, files):
    """
    Stop all child processes and close their log files.
    """
    for name, proc in processes.items():
        if proc.poll() is None:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                proc.wait(timeout=3)
            except Exception:
                proc.kill()

    for f in files.values():
        try:
            f.close()
        except Exception:
            pass

def trace_loop(pid_list, use_gpu, sample_rate):
    """
    Main tracing loop. Launch scripts and monitor until interrupted.
    """
    processes = {}
    files = {}

    for pid in pid_list:
        pid_str = str(pid)
        for script, cfg in SCRIPTS.items():
            # Skip GPU-related scripts if GPU tracing is disabled
            if (("cuda" in script) or ("nccl" in script)) and not use_gpu:
                continue
            proc, f = launch_script(script, pid_str, cfg)
            if proc:
                key = f"{script}:{pid_str}"
                processes[key] = proc
                files[key] = f

    print(f"[monitor] started {len(processes)} scripts for {len(pid_list)} PID(s).", file=sys.stderr)

    while not stop:
        for name, proc in list(processes.items()):
            if proc.poll() is not None:
                print(f"[warn] {name} exited (code={proc.returncode})", file=sys.stderr)
                processes.pop(name)
                f = files.pop(name, None)
                if f:
                    f.close()
        time.sleep(1)

    stop_all(processes, files)
    print("[monitor] stopped.", file=sys.stderr)

def read_pid_file(path):
    """
    Read newline-separated PIDs from file.
    """
    if not os.path.exists(path):
        print(f"[error] PID file not found: {path}", file=sys.stderr)
        sys.exit(1)

    pids = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                pids.append(int(line))
            except ValueError:
                print(f"[warn] ignoring invalid PID entry: {line}", file=sys.stderr)
    return pids

def main():

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "-p", "--pid",
        type=int,
        action="append",
        help="Target PID(s) to trace."
    )

    parser.add_argument(
        "--pid-file",
        type=str,
        default=DEFAULT_PID_FILE,
        help=f"Path to newline-separated PID file (default: {DEFAULT_PID_FILE})"
    )

    parser.add_argument(
        "-g", "--gpu",
        action="store_true",
        help="Trace ML workloads."
    )

    parser.add_argument(
        "-s", "--sample-rate",
        type=int,
        default=500000,
        help="Sample rate (ns)."
    )

    parser.add_argument(
        "--script-dir", 
        type=str, 
        default=SCRIPT_DIR,
        help=f"Directory containing event scripts (default: {SCRIPT_DIR})")

    args = parser.parse_args()

    pid_list = []

    # 1. Read from file if it exists or is provided
    file_pids = read_pid_file(args.pid_file)
    pid_list.extend(file_pids)

    # 2. Add PIDs passed via CLI
    if args.pid:
        pid_list.extend(args.pid)

    # 3. Remove duplicates
    pid_list = sorted(set(pid_list))

    if not pid_list:
        print("[error] No valid PIDs provided.", file=sys.stderr)
        sys.exit(1)

    for name, cfg in SCRIPTS.items():
        if "-s" in cfg["args"]:
            idx = cfg["args"].index("-s")
            cfg["args"][idx + 1] = str(args.sample_rate)


    trace_loop(pid_list, args.gpu, args.sample_rate)

if __name__ == "__main__":
    main()