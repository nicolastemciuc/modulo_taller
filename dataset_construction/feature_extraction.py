#!/usr/bin/env python3
import argparse
import csv
import os
from bisect import bisect_right
from pathlib import Path
from typing import Dict, List, Tuple, Optional

def load_feature_timeline(path: Path) -> List[Tuple[int, float]]:
    """
    Load a polling feature file with header: ts_ns,value
    Returns a list of (ts_ns:int, value:float) sorted by ts_ns.
    """
    timeline: List[Tuple[int, float]] = []
    if not path.exists():
        return timeline
    with open(path, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        # Allow both lower/upper case headers if needed
        has_ts = "ts_ns" in r.fieldnames
        has_val = "value" in r.fieldnames
        if not (has_ts and has_val):
            return timeline
        for row in r:
            try:
                t = int(row["ts_ns"])
                v_raw = row["value"].strip()
                v = float(v_raw) if (("." in v_raw) or ("e" in v_raw.lower())) else float(int(v_raw))
                timeline.append((t, v))
            except Exception:
                continue
    timeline.sort(key=lambda x: x[0])
    return timeline

def last_value_at_or_before(timeline: List[Tuple[int, float]], t_ns: int) -> Optional[float]:
    """
    Binary search: return value where ts_ns <= t_ns (the rightmost such).
    If none, return None.
    """
    if not timeline:
        return None
    times = [x[0] for x in timeline]
    idx = bisect_right(times, t_ns) - 1
    if idx < 0:
        return None
    return timeline[idx][1]

def detect_feature_files(polling_dir: Path, include_pid_files: bool) -> Dict[str, Path]:
    """
    Discover feature csvs in polling_dir.
    - Skips per-PID files by default (files matching *.pid*.csv) unless include_pid_files=True.
    - Returns mapping feature_name -> path, where feature_name is derived from filename (without .csv / pid suffix).
    """
    out: Dict[str, Path] = {}
    for p in sorted(polling_dir.glob("*.csv")):
        name = p.name
        if (".pid" in name) and not include_pid_files:
            continue
        # Feature name is file stem up to first ".pid"
        stem = p.stem
        feat = stem.split(".pid")[0]
        out[feat] = p
    return out

def main():
    ap = argparse.ArgumentParser(
        description="Join flow_extraction.csv with polling feature timelines by taking the last polled value before flow start."
    )
    ap.add_argument("--flows", default="flow_extraction.csv",
                    help="Path to flows CSV with start_ts_ns column (default: flow_extraction.csv)")
    ap.add_argument("--polling-dir", default="../trace_collector/polling_traces",
                    help="Directory containing per-feature polling CSVs (default: ../trace_collector/polling_traces)")
    ap.add_argument("--features", nargs="*",
                    help="Explicit list of feature CSV files to use (paths). If omitted, auto-detect *.csv in polling-dir.")
    ap.add_argument("--include-pid-files", action="store_true",
                    help="Include files like feature.pid1234.csv (disabled by default).")
    ap.add_argument("--output", default="feature_extraction.csv",
                    help="Output CSV (default: feature_extraction.csv)")
    args = ap.parse_args()

    flows_path = Path(args.flows)
    polling_dir = Path(args.polling_dir)
    out_path = Path(args.output)

    if not flows_path.exists():
        raise SystemExit(f"[error] flows file not found: {flows_path}")

    # Load feature timelines
    feature_files: Dict[str, Path] = {}
    if args.features:
        # Use user-specified files; derive feature names from filenames
        for f in args.features:
            p = Path(f)
            if not p.exists():
                print(f"[warn] feature file not found: {p}", file=os.sys.stderr)
                continue
            feat = p.stem.split(".pid")[0]
            feature_files[feat] = p
    else:
        feature_files = detect_feature_files(polling_dir, include_pid_files=args.include_pid_files)

    if not feature_files:
        raise SystemExit(f"[error] no feature files found in {polling_dir} (or none passed via --features)")

    timelines: Dict[str, List[Tuple[int, float]]] = {}
    for feat, path in feature_files.items():
        tl = load_feature_timeline(path)
        if not tl:
            print(f"[warn] empty or invalid feature file skipped: {path}", file=os.sys.stderr)
            continue
        timelines[feat] = tl

    if not timelines:
        raise SystemExit("[error] no valid feature timelines loaded")

    # Read flows and enrich
    with open(flows_path, "r", newline="", encoding="utf-8") as f_in, \
         open(out_path, "w", newline="", encoding="utf-8") as f_out:

        r = csv.DictReader(f_in)
        fieldnames = list(r.fieldnames or [])
        # Ensure start_ts_ns exists; if not, try to derive from start_time (H:M:S) — but we expect ts
        if "start_ts_ns" not in fieldnames:
            raise SystemExit("[error] flows file missing 'start_ts_ns' column. Re-run flow_extraction.py that emits it.")

        # Add feature columns (use the feature names as column names)
        extra_cols = sorted(timelines.keys())
        out_fields = fieldnames + extra_cols
        w = csv.DictWriter(f_out, fieldnames=out_fields)
        w.writeheader()

        missing_before_count: Dict[str, int] = {k: 0 for k in extra_cols}
        row_count = 0

        for row in r:
            row_count += 1
            try:
                start_ts_ns = int(row["start_ts_ns"])
            except Exception:
                # if invalid, leave features blank
                start_ts_ns = None

            # Compute feature values
            for feat in extra_cols:
                val: Optional[float] = None
                if start_ts_ns is not None:
                    val = last_value_at_or_before(timelines[feat], start_ts_ns)
                if val is None:
                    # no value before flow start
                    missing_before_count[feat] += 1
                    row[feat] = ""
                else:
                    # Write as int if it is an integer, else float
                    row[feat] = int(val) if float(val).is_integer() else f"{val:.6f}"

            w.writerow(row)

    # Summary to stderr
    print(f"[join] wrote: {out_path}", file=os.sys.stderr)
    for feat, cnt in missing_before_count.items():
        if cnt:
            print(f"[join] {feat}: {cnt} flows had no prior sample (left blank)", file=os.sys.stderr)

if __name__ == "__main__":
    main()
