#!/usr/bin/env python3
import os
import glob
import pandas as pd

K = 5

def process_one(inp_path: str, out_path: str):
    df = pd.read_csv(inp_path)

    key = ["src_ip", "src_port", "dst_ip", "dst_port"]
    if "start_ts_ns" not in df.columns:
        raise SystemExit(f"{inp_path}: missing 'start_ts_ns'")

    df = df.sort_values(key + ["start_ts_ns"], kind="mergesort")

    skip = {
        "flow_id", "start_time", "end_time", "start_ts_ns",
        "src_ip", "src_port", "dst_ip", "dst_port",
    }

    numeric_cols = []
    for col in df.columns:
        if col in skip:
            continue
        c = pd.to_numeric(df[col], errors="coerce")
        if c.notna().any():
            numeric_cols.append(col)

    g = df.groupby(key, sort=False)
    for k in range(1, K + 1):
        for col in numeric_cols:
            df[f"{col}_t{k}"] = g[col].shift(k)

    df.to_csv(out_path, index=False)
    print(f"[hist] wrote {out_path}")

def main():
    base = os.path.dirname(os.path.abspath(__file__))
    pattern = os.path.join(base, "feature_extraction_pid*.csv")
    files = sorted(glob.glob(pattern))

    if not files:
        inp = os.path.join(base, "feature_extraction.csv")
        out = os.path.join(base, "historical_information.csv")
        if not os.path.exists(inp):
            raise SystemExit("[hist] no per-PID feature files and no feature_extraction.csv")
        process_one(inp, out)
        return

    for inp in files:
        stem = os.path.splitext(os.path.basename(inp))[0]
        pid_part = ""
        prefix = "feature_extraction_pid"
        if stem.startswith(prefix):
            pid_part = stem[len(prefix):]
        else:
            pid_part = stem

        out = os.path.join(base, f"historical_information_pid{pid_part}.csv")
        process_one(inp, out)

if __name__ == "__main__":
    main()
