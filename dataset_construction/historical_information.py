#!/usr/bin/env python3
import os, sys
import pandas as pd
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from config_loader import CFG

K = CFG["historical_information"]["k"]
FEATURE_EXTRACTION_CSV = REPO_ROOT / CFG["feature_extraction"]["output_csv"]
HISTORICAL_INFORMATION_CSV = REPO_ROOT / CFG["historical_information"]["output_csv"]

def main():
    base = os.path.dirname(os.path.abspath(__file__))
    inp = FEATURE_EXTRACTION_CSV
    out = HISTORICAL_INFORMATION_CSV

    df = pd.read_csv(inp)

    key = ["src_ip", "src_port", "dst_ip", "dst_port"]
    if "start_ts_ns" not in df.columns:
        raise SystemExit("missing 'start_ts_ns'")

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

    df.to_csv(out, index=False)

if __name__ == "__main__":
    main()
