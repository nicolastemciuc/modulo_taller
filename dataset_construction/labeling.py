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

def process_one_file(inp_path: Path, out_path: Path):
    df = pd.read_csv(os.path.abspath(inp_path))

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

    df.to_csv(os.path.abspath(out_path), index=False)
    print(f"[label] wrote {out_path}", file=sys.stderr)


def main():
    inp_base = Path(INP)
    out_base = Path(OUT)

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
        process_one_file(inp_base, out_base)


if __name__ == "__main__":
    main()
