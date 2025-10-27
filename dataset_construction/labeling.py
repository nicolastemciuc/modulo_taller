#!/usr/bin/env python3
import os
import pandas as pd

INP  = "historical_information.csv"
OUT  = "final.csv"
KEYS = ["src_ip"]
ELEPHANT_FRAC = 0.10
LINK_MBPS = 1000.0

def main():
    base = os.path.dirname(os.path.abspath(__file__))
    df = pd.read_csv(os.path.join(base, INP))

    needed = {"start_ts_ns", "start_time", "end_time", "total_size"}
    missing = needed - set(df.columns)
    if missing:
        raise SystemExit(f"Missing: {', '.join(sorted(missing))}")

    df = df.sort_values(KEYS + ["start_ts_ns"], kind="mergesort")

    start_s = pd.to_numeric(df["start_time"], errors="coerce")
    end_s   = pd.to_numeric(df["end_time"], errors="coerce")
    dur_s   = (end_s - start_s).where((end_s - start_s) > 0, pd.NA)

    size_B = pd.to_numeric(df["total_size"], errors="coerce")
    df["duration_s"]  = dur_s
    df["avg_rate_bps"] = (size_B * 8) / dur_s

    thr_bps = ELEPHANT_FRAC * (LINK_MBPS * 1_000_000.0)
    df["y_is_elephant"] = (df["avg_rate_bps"] > thr_bps).astype("Int64")

    g = df.groupby(KEYS, sort=False)
    df["y_next_size"] = pd.to_numeric(g["total_size"].shift(-1), errors="coerce").astype("Int64")

    df.to_csv(os.path.join(base, OUT), index=False)

if __name__ == "__main__":
    main()
