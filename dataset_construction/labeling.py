#!/usr/bin/env python3
import os
import glob
import pandas as pd

INP_SINGLE  = "historical_information.csv"
OUT_SINGLE  = "final.csv"
KEYS = ["src_ip"]
ELEPHANT_FRAC = 0.10
LINK_MBPS = 1000.0

def label_one(inp_path: str, out_path: str):
    df = pd.read_csv(inp_path)

    needed = {"start_ts_ns", "start_time", "end_time", "total_size"}
    missing = needed - set(df.columns)
    if missing:
        raise SystemExit(f"{inp_path}: Missing: {', '.join(sorted(missing))}")

    df = df.sort_values(KEYS + ["start_ts_ns"], kind="mergesort")

    start_s = pd.to_numeric(df["start_time"], errors="coerce")
    end_s   = pd.to_numeric(df["end_time"], errors="coerce")
    dur_s   = (end_s - start_s).where((end_s - start_s) > 0, pd.NA)

    size_B = pd.to_numeric(df["total_size"], errors="coerce")
    df["duration_s"]   = dur_s
    df["avg_rate_bps"] = (size_B * 8) / dur_s

    thr_bps = ELEPHANT_FRAC * (LINK_MBPS * 1_000_000.0)
    df["y_is_elephant"] = (df["avg_rate_bps"] > thr_bps).astype("Int64")

    g = df.groupby(KEYS, sort=False)
    df["y_next_size"] = pd.to_numeric(g["total_size"].shift(-1), errors="coerce").astype("Int64")

    df.to_csv(out_path, index=False)
    print(f"[label] wrote {out_path}")

def main():
    base = os.path.dirname(os.path.abspath(__file__))
    pattern = os.path.join(base, "historical_information_pid*.csv")
    files = sorted(glob.glob(pattern))

    if not files:
        inp = os.path.join(base, INP_SINGLE)
        out = os.path.join(base, OUT_SINGLE)
        if not os.path.exists(inp):
            raise SystemExit("[label] no per-PID historical files and no historical_information.csv")
        label_one(inp, out)
        return

    for inp in files:
        stem = os.path.splitext(os.path.basename(inp))[0]
        pid_part = ""
        prefix = "historical_information_pid"
        if stem.startswith(prefix):
            pid_part = stem[len(prefix):]
        else:
            pid_part = stem

        out = os.path.join(base, f"final_pid{pid_part}.csv")
        label_one(inp, out)

if __name__ == "__main__":
    main()
