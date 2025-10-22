#!/usr/bin/env python3
import argparse
import os
import pandas as pd

DEFAULT_K = 5

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_input = os.path.join(script_dir, "feature_extraction.csv")
    default_output = os.path.join(script_dir, "historical_information.csv")

    p = argparse.ArgumentParser(description="Add K-lag historical features per 4-tuple (src_ip,src_port,dst_ip,dst_port).")
    p.add_argument("--input", default=default_input, help=f"Input CSV file (default: {default_input})")
    p.add_argument("--output", default=default_output, help=f"Output CSV file (default: {default_output})")
    p.add_argument("--k", type=int, default=DEFAULT_K, help=f"Number of previous flows to include (default: {DEFAULT_K})")
    args = p.parse_args()

    print(f"Reading: {args.input}")
    print(f"Writing: {args.output}")
    print(f"K: {args.k}")

    df = pd.read_csv(args.input)

    key_cols = ["src_ip", "src_port", "dst_ip", "dst_port"]
    if "start_ts_ns" not in df.columns:
        raise SystemExit("Column 'start_ts_ns' is required to order flows in time.")

    df = df.sort_values(key_cols + ["start_ts_ns"], kind="mergesort")

    do_not_lag = set([
        "flow_id", "start_time", "end_time", "start_ts_ns",
        "src_ip", "src_port", "dst_ip", "dst_port",
    ])

    numeric_candidates = []
    for col in df.columns:
        if col in do_not_lag:
            continue
        c = pd.to_numeric(df[col], errors="coerce")
        if c.notna().any():
            numeric_candidates.append(col)

    g = df.groupby(key_cols, sort=False)
    for k in range(1, args.k + 1):
        for col in numeric_candidates:
            df[f"{col}_t{k}"] = g[col].shift(k)

    df.to_csv(args.output, index=False)
    print(f"Wrote: {args.output}")
    print(f"Added historical features for K={args.k} over columns: {', '.join(numeric_candidates)}")

if __name__ == "__main__":
    main()
