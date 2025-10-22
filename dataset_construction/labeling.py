#!/usr/bin/env python3
import argparse
import os
import math
import pandas as pd

# ---- Defaults ----
DEFAULT_INPUT = "historical_information.csv"
DEFAULT_OUTPUT = "final.csv"
DEFAULT_GROUPING = "host"   # choices: host, 4tuple
DEFAULT_ELEPHANT_FRAC = 0.10
DEFAULT_LINK_MBPS = 1000.0  # 1 Gbps

KEYS_BY_MODE = {
    "host":   ["src_ip"],  # next flow from the same host (default)
    "4tuple": ["src_ip", "src_port", "dst_ip", "dst_port"],  # next flow in the same 4-tuple
}

def positive_float(v:str) -> float:
    x = float(v)
    if x <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return x

def parse_args():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    p = argparse.ArgumentParser(
        description="Create regression and classification labels from historical_information.csv"
    )
    p.add_argument("--input", default=os.path.join(script_dir, DEFAULT_INPUT),
                   help=f"Input CSV (default: {DEFAULT_INPUT})")
    p.add_argument("--output", default=os.path.join(script_dir, DEFAULT_OUTPUT),
                   help=f"Output CSV with labels (default: {DEFAULT_OUTPUT})")
    p.add_argument("--group-by", choices=KEYS_BY_MODE.keys(), default=DEFAULT_GROUPING,
                   help="How to define 'next' flow (default: host)")
    p.add_argument("--elephant-frac", type=positive_float, default=DEFAULT_ELEPHANT_FRAC,
                   help="Fraction of link capacity to call an elephant (default: 0.10)")
    p.add_argument("--link-mbps", type=positive_float, default=DEFAULT_LINK_MBPS,
                   help="Nominal link capacity in Mbps (default: 1000)")
    p.add_argument("--link-gbps", type=positive_float,
                   help="Alternative to --link-mbps; if provided, overrides it")
    p.add_argument("--drop-missing-targets", action="store_true",
                   help="Drop rows where y_next_size is NaN (last flow in each group).")
    return p.parse_args()

def main():
    args = parse_args()

    link_mbps = args.link_mbps
    if args.link_gbps is not None:
        link_mbps = args.link_gbps * 1000.0
    link_bps = link_mbps * 1_000_000.0
    elephant_threshold_bps = args.elephant_frac * link_bps

    print(f"Reading:  {args.input}")
    print(f"Writing:  {args.output}")
    print(f"Grouping: {args.group_by}  (keys = {KEYS_BY_MODE[args.group_by]})")
    print(f"Elephant threshold: {args.elephant_frac*100:.1f}% of {link_mbps:.1f} Mbps = {elephant_threshold_bps:.0f} bps")

    df = pd.read_csv(args.input)

    # Sanity checks
    needed_cols = {"start_ts_ns", "start_time", "end_time", "total_size"}
    missing = needed_cols - set(df.columns)
    if missing:
        raise SystemExit(f"Missing required columns: {', '.join(sorted(missing))}")

    # Ensure proper ordering
    key_cols = KEYS_BY_MODE[args.group_by]
    df = df.sort_values(key_cols + ["start_ts_ns"], kind="mergesort")

    # Duration & rate
    # start_time / end_time in your sample look like epoch seconds with microseconds -> parse as float
    start_s = pd.to_numeric(df["start_time"], errors="coerce")
    end_s   = pd.to_numeric(df["end_time"], errors="coerce")
    duration_s = end_s - start_s
    # Avoid division by zero: where duration <= 0, set to NaN so rate becomes NaN
    duration_s = duration_s.where(duration_s > 0, pd.NA)

    total_size_bytes = pd.to_numeric(df["total_size"], errors="coerce")
    avg_rate_bps = (total_size_bytes * 8) / duration_s  # bits per second

    df["duration_s"] = duration_s
    df["avg_rate_bps"] = avg_rate_bps

    # Classification label: elephant if avg_rate_bps > threshold
    df["y_is_elephant"] = (df["avg_rate_bps"] > elephant_threshold_bps).astype("Int64")

    # Regression target: next flow size (by group)
    g = df.groupby(key_cols, sort=False)
    df["y_next_size"] = g["total_size"].shift(-1)  # next flow’s size within group
    # Keep as float if NaN present; cast to Int64 (nullable) for nicer CSV integers
    df["y_next_size"] = pd.to_numeric(df["y_next_size"], errors="coerce").astype("Int64")

    if args.drop_missing_targets:
        before = len(df)
        df = df[df["y_next_size"].notna()]
        after = len(df)
        print(f"Dropped {before - after} rows with missing y_next_size (last row in each group).")

    df.to_csv(args.output, index=False)
    print("Done.")

if __name__ == "__main__":
    main()
