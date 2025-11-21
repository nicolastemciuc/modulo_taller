import csv, os, sys
from bisect import bisect_right
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from config_loader import CFG

FLOWS_PATH  = REPO_ROOT / CFG["flow_extraction"]["output_csv"]
POLLING_DIR = REPO_ROOT / CFG["polling_traces"]["output_dir"]
EVENTS_DIR  = REPO_ROOT / CFG["event_traces"]["output_dir"]
OUT_PATH    = REPO_ROOT / CFG["feature_extraction"]["output_csv"]


def load_polled_timeline(path: Path):
    tl = []
    if not path.exists():
        return tl
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
            except:
                pass
    tl.sort(key=lambda x: x[0])
    return tl


def detect_polled_feature_files(polling_dir: Path):
    grouped = {}
    for p in sorted(polling_dir.glob("*.csv")):
        name = p.name
        if name == "memavailable_bytes.csv":
            base = "memavailable_bytes"
        else:
            base = p.stem.split("_pid")[0]
        grouped.setdefault(base, []).append(p)
    return grouped


def load_polled_timelines(grouped_files: dict[str, list[Path]]):
    timelines = {}
    for feat, files in grouped_files.items():
        accum = {}
        for fp in files:
            tl = load_polled_timeline(fp)
            for t, v in tl:
                accum[t] = accum.get(t, 0.0) + v
        if accum:
            merged = sorted(accum.items(), key=lambda x: x[0])
            timelines[feat] = merged
    return timelines


def parse_event_csv_rows(path: Path):
    if not path.exists() or path.stat().st_size == 0:
        return []
    rows = []
    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            rd = csv.reader(f)
            for raw in rd:
                if len(raw) < 2:
                    continue
                ts_s, val_s = raw[0].strip(), raw[1].strip()
                try:
                    t = int(ts_s)
                    v = float(val_s) if ("." in val_s or "e" in val_s.lower()) else float(int(val_s))
                    rows.append((t, v))
                except:
                    continue
    except:
        pass
    return rows


def load_event_timelines(events_dir: Path):
    grouped = {}
    for p in sorted(events_dir.glob("*.csv")):
        base = p.stem.split("_pid")[0]
        grouped.setdefault(base, []).append(p)

    timelines = {}
    for evt, files in grouped.items():
        merged = []
        for fp in files:
            merged.extend(parse_event_csv_rows(fp))
        if merged:
            merged.sort(key=lambda x: x[0])
            timelines[evt] = merged
    return timelines


def last_sample_at_or_before(tl, t_ns):
    if not tl:
        return None
    times = [x[0] for x in tl]
    i = bisect_right(times, t_ns) - 1
    return None if i < 0 else tl[i]


def main():
    if not FLOWS_PATH.exists():
        raise SystemExit(f"[error] flows file not found: {FLOWS_PATH}")

    polled_grouped = detect_polled_feature_files(POLLING_DIR) if POLLING_DIR.exists() else {}
    polled_tl = load_polled_timelines(polled_grouped)
    polled_tl = {k: v for k, v in polled_tl.items() if v}

    event_tl = load_event_timelines(EVENTS_DIR) if EVENTS_DIR.exists() else {}

    if not polled_tl and not event_tl:
        raise SystemExit("[error] no valid timelines (polling/events) loaded")

    with open(FLOWS_PATH, "r", newline="", encoding="utf-8") as fin, \
         open(OUT_PATH, "w", newline="", encoding="utf-8") as fout:

        r = csv.DictReader(fin)
        fields = list(r.fieldnames or [])
        if "start_ts_ns" not in fields:
            raise SystemExit("[error] flows file missing 'start_ts_ns'")

        polled_fields = sorted(polled_tl.keys())
        event_fields  = sorted(event_tl.keys())

        extra_fields = polled_fields + [e for evt in event_fields for e in (evt, f"tt_{evt}")]
        w = csv.DictWriter(fout, fieldnames=fields + extra_fields)
        w.writeheader()

        missing_polled     = {k: 0 for k in polled_fields}
        missing_event_val  = {k: 0 for k in event_fields}

        for row in r:
            try:
                start_ts_ns = int(row["start_ts_ns"])
            except:
                start_ts_ns = None

            for feat in polled_fields:
                if start_ts_ns is None:
                    row[feat] = ""
                    missing_polled[feat] += 1
                    continue
                tl = polled_tl[feat]
                times = [x[0] for x in tl]
                i = bisect_right(times, start_ts_ns) - 1
                if i < 0:
                    row[feat] = ""
                    missing_polled[feat] += 1
                else:
                    val = tl[i][1]
                    row[feat] = int(val) if float(val).is_integer() else f"{val:.6f}"

            for evt in event_fields:
                val_key, tt_key = evt, f"tt_{evt}"
                row[val_key] = ""
                row[tt_key] = ""
                if start_ts_ns is None:
                    missing_event_val[evt] += 1
                    continue
                pair = last_sample_at_or_before(event_tl[evt], start_ts_ns)
                if pair is None:
                    missing_event_val[evt] += 1
                else:
                    t_e, v_e = pair
                    row[val_key] = int(v_e) if float(v_e).is_integer() else f"{v_e:.6f}"
                    dt_s = max(0, (start_ts_ns - t_e) / 1_000_000_000.0)
                    row[tt_key] = f"{dt_s:.6f}"

            w.writerow(row)

    print(f"[join] wrote: {OUT_PATH}", file=os.sys.stderr)
    for feat, cnt in (polled_fields and missing_polled or {}).items():
        if cnt:
            print(f"[join] polled {feat}: {cnt} missing", file=os.sys.stderr)
    for evt, cnt in missing_event_val.items():
        if cnt:
            print(f"[join] event {evt}: {cnt} missing", file=os.sys.stderr)


if __name__ == "__main__":
    main()
