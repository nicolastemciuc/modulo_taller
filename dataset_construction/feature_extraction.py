#!/usr/bin/env python3
import csv, os
from bisect import bisect_right
from pathlib import Path

FLOWS_PATH = Path("flow_extraction.csv")
POLLING_DIR = Path("../trace_collector/polling_traces")
OUT_PATH = Path("feature_extraction.csv")

def load_feature_timeline(path: Path):
    tl = []
    if not path.exists(): return tl
    with open(path, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        if not r.fieldnames or "ts_ns" not in r.fieldnames or "value" not in r.fieldnames:
            return tl
        for row in r:
            try:
                t = int(row["ts_ns"])
                v_raw = row["value"].strip()
                v = float(v_raw) if ("." in v_raw or "e" in v_raw.lower()) else float(int(v_raw))
                tl.append((t, v))
            except:  # ignore bad rows
                pass
    tl.sort(key=lambda x: x[0])
    return tl

def last_value_at_or_before(tl, t_ns):
    if not tl: return None
    times = [x[0] for x in tl]
    i = bisect_right(times, t_ns) - 1
    return None if i < 0 else tl[i][1]

def detect_feature_files(polling_dir: Path):
    out = {}
    for p in sorted(polling_dir.glob("*.csv")):
        if ".pid" in p.name:  # skip per-PID files
            continue
        feat = p.stem.split(".pid")[0]
        out[feat] = p
    return out

def main():
    if not FLOWS_PATH.exists():
        raise SystemExit(f"[error] flows file not found: {FLOWS_PATH}")
    feat_files = detect_feature_files(POLLING_DIR)
    if not feat_files:
        raise SystemExit(f"[error] no feature files found in {POLLING_DIR}")
    timelines = {feat: load_feature_timeline(path) for feat, path in feat_files.items()}
    timelines = {k:v for k,v in timelines.items() if v}
    if not timelines:
        raise SystemExit("[error] no valid feature timelines loaded")

    with open(FLOWS_PATH, "r", newline="", encoding="utf-8") as fin, \
         open(OUT_PATH, "w", newline="", encoding="utf-8") as fout:
        r = csv.DictReader(fin)
        fields = list(r.fieldnames or [])
        if "start_ts_ns" not in fields:
            raise SystemExit("[error] flows file missing 'start_ts_ns'")
        extras = sorted(timelines.keys())
        w = csv.DictWriter(fout, fieldnames=fields + extras)
        w.writeheader()

        missing = {k:0 for k in extras}
        for row in r:
            try:
                start_ts_ns = int(row["start_ts_ns"])
            except:
                start_ts_ns = None
            for feat in extras:
                val = last_value_at_or_before(timelines[feat], start_ts_ns) if start_ts_ns is not None else None
                if val is None:
                    missing[feat] += 1
                    row[feat] = ""
                else:
                    row[feat] = int(val) if float(val).is_integer() else f"{val:.6f}"
            w.writerow(row)

    print(f"[join] wrote: {OUT_PATH}", file=os.sys.stderr)
    for feat, cnt in missing.items():
        if cnt:
            print(f"[join] {feat}: {cnt} flows had no prior sample", file=os.sys.stderr)

if __name__ == "__main__":
    main()
