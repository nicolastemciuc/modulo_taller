#!/usr/bin/env python3
import argparse, os, sys, signal, subprocess, time

SELF = os.getpid()
SCRIPT_DIR = "./python/"
RECORD_DIR = "./records/"
RATE_NS = "500000"

SCRIPTS = {
    "cuda_mem.py": {"args": ["-s", RATE_NS], "record": "cuda_allocations"},
    "cpu_mem.py": {"args": ["-s", RATE_NS], "record": "cpu_allocations"},
    "nccl_mem.py": {"args": ["-s", RATE_NS], "record": "cuda_collective"},
    "kcache.py": {"args": [], "record": "kcache"},
    "sendmsg.py": {"args": [], "record": "sendmsg"},
    "sendto.py": {"args": [], "record": "sendto"},
    "tcpsendmsg.py": {"args": [], "record": "tcpsendmsg"},
    "write.py": {"args": [], "record": "write"},
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
    script_path = os.path.join(SCRIPT_DIR, script)
    log_path = os.path.join(RECORD_DIR, cfg["record"])
    create_log_dir(log_path)

    cmd = ["sudo", sys.executable, script_path, "-p", pid_str] + cfg["args"]
    try:
        log_file = open(log_path, "a")
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
    for name, proc in processes.items():
        if proc.poll() is None:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                proc.wait(timeout=3)
            except Exception:
                proc.kill()
    for f in files.values():
        try: f.close()
        except: pass

def trace_loop(pid, use_gpu, sample_rate):
    pid_str = str(pid)
    processes, files = {}, {}

    for script, cfg in SCRIPTS.items():
        if script == "cuda_mem.py"  and not use_gpu:
            continue
        proc, f = launch_script(script, pid_str, cfg)
        if proc:
            processes[script] = proc
            files[script] = f

    print(f"[monitor] started {len(processes)} scripts for PID {pid}.", file=sys.stderr)

    while not stop:
        for name, proc in list(processes.items()):
            if proc.poll() is not None:
                print(f"[warn] {name} exited (code={proc.returncode})", file=sys.stderr)
                processes.pop(name)
                files.pop(name, None).close()
        time.sleep(1)

    stop_all(processes, files)
    print("[monitor] stopped.", file=sys.stderr)

def main():

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("-p", "--pid", type=int, action="append", default=-1)
    parser.add_argument("-g", "--gpu", action="store_true")
    parser.add_argument("-s", "--sample-rate", type=int, default=500000)

    args = parser.parse_args()

    if args.pid <= 0:
        print("Invalid PID.", file=sys.stderr)
        sys.exit(1)

    trace_loop(args.pid, args.gpu, args.sample_rate)

if __name__ == "__main__":
    main()