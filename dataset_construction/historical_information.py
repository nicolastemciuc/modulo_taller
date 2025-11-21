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


def process_one_file(inp_path: Path, out_path: Path):
    df = pd.read_csv(inp_path)

    key = ["src_ip", "src_port", "dst_ip", "dst_port"]
    for kcol in key:
        if kcol not in df.columns:
            raise SystemExit(f"{inp_path}: missing '{kcol}'")

    if "start_ts_ns" not in df.columns:
        raise SystemExit(f"{inp_path}: missing 'start_ts_ns'")

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

    df = df.sort_values(key + ["start_ts_ns"], kind="mergesort").copy()

    g = df.groupby(key, sort=False)
    for k in range(1, K + 1):
        for col in numeric_cols:
            df[f"{col}_t{k}"] = g[col].shift(k)

    df.to_csv(out_path, index=False)
    print(f"[hist] wrote {out_path}", file=sys.stderr)


def main():
    inp_base = Path(FEATURE_EXTRACTION_CSV)
    out_base = Path(HISTORICAL_INFORMATION_CSV)

    in_dir   = inp_base.parent
    in_stem  = inp_base.stem
    in_suf   = inp_base.suffix

    out_dir  = out_base.parent
    out_stem = out_base.stem
    out_suf  = out_base.suffix

    candidates = sorted(in_dir.glob(f"{in_stem}_pid*{in_suf}"))

    if candidates:
        for inp_path in candidates:
            stem = inp_path.stem
            if "_pid" in stem:
                pid_part = stem.split("_pid", 1)[1]
            else:
                pid_part = "unknown"
            out_path = out_dir / f"{out_stem}_pid{pid_part}{out_suf}"
            process_one_file(inp_path, out_path)
    else:
        # Single global dataset fallback
        process_one_file(inp_base, out_base)


if __name__ == "__main__":
    main()
