#!/usr/bin/env python3
import os, sys
import pandas as pd
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from config_loader import CFG

INP  = REPO_ROOT / CFG["historical_information"]["output_csv"]
OUT  = REPO_ROOT / CFG["labeling"]["output_csv"]
KEYS = ["src_ip"]
ELEPHANT_FRAC = CFG["labeling"]["elephant_fraction"]
LINK_MBPS = 1000.0

def main():
    df = pd.read_csv(os.path.abspath(INP))

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

    df.to_csv(os.path.abspath(OUT), index=False)

if __name__ == "__main__":
    main()
