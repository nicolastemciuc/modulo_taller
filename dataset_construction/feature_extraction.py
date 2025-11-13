#!/usr/bin/env python3
import csv, os
from bisect import bisect_right
from pathlib import Path

FLOWS_PATH   = Path("flow_extraction.csv")
POLLING_DIR  = Path("../trace_collector/polling_traces")
EVENTS_DIR   = Path("../trace_collector/event_traces")
OUT_PREFIX   = "feature_extraction_pid"

PID_FIELD_CANDIDATES = ["pid", "workload_pid", "spark_pid"]

def load_polled_timeline(paths):
    merged = []
    for path in paths:
        if not path or not path.exists():
            continue
        with open(path, "r", newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            if not r.fieldnames or "ts_ns" not in r.fieldnames or "value" not in r.fieldnames:
                continue
            for row in r:
                try:
                    t = int(row["ts_ns"])
                    v_raw = row["value"].strip()
                    v = float(v_raw) if ("." in v_raw or "e" in v_raw.lower()) else float(int(v_raw))
                    merged.append((t, v))
                except:
                    pass

    if not merged:
        return []
    merged.sort(key=lambda x: x[0])
    return merged

def detect_polled_feature_files_per_pid(polling_dir: Path):
    """
    polling_traces:
      memavailable_bytes.csv
      disk_read_bytes_247417.csv
      vmrss_bytes_256361.csv
      ...
    Returns:
      mem_path: Path or None
      per_pid: dict[int, dict[str, Path]]
    """
    per_pid = {}
    mem_path = None

    for p in sorted(polling_dir.glob("*.csv")):
        stem = p.stem

        if stem == "memavailable_bytes":
            mem_path = p
            continue

        if "_" not in stem:
            continue

        metric, pid_part = stem.rsplit("_", 1)
        if not pid_part.isdigit():
            continue

        pid = int(pid_part)
        per_pid.setdefault(pid, {})[metric] = p

    return mem_path, per_pid

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

def load_event_timelines_per_pid(events_dir: Path):
    """
    events:
      cpu_allocations_pid247417.csv
      cpu_allocations_pid247430.csv
      ...
    Returns:
      event_tl[pid][event] -> [(t_ns, value), ...]
    """
    event_tl = {}

    for p in sorted(events_dir.glob("*.csv")):
        stem = p.stem
        if "_pid" not in stem:
            continue

        base, pid_part = stem.rsplit("_pid", 1)
        if not pid_part.isdigit():
            continue
        pid = int(pid_part)

        rows = parse_event_csv_rows(p)
        if not rows:
            continue

        event_tl.setdefault(pid, {}).setdefault(base, []).extend(rows)

    # sort
    for pid, evts in event_tl.items():
        for evt, rows in evts.items():
            rows.sort(key=lambda x: x[0])

    return event_tl

def last_sample_at_or_before(tl, t_ns):
    if not tl:
        return None
    times = [x[0] for x in tl]
    i = bisect_right(times, t_ns) - 1
    return None if i < 0 else tl[i]

def main():
    if not FLOWS_PATH.exists():
        raise SystemExit(f"[error] flows file not found: {FLOWS_PATH}")

    # Load flows once
    with open(FLOWS_PATH, "r", newline="", encoding="utf-8") as fin:
        r = csv.DictReader(fin)
        fields = list(r.fieldnames or [])
        if "start_ts_ns" not in fields:
            raise SystemExit("[error] flows file missing 'start_ts_ns'")
        flows = list(r)

    if not flows:
        raise SystemExit("[error] no flows loaded from flows file")

    # Detect PID column in flows, if any
    pid_field = next((f for f in PID_FIELD_CANDIDATES if f in fields), None)

    flows_by_pid = {}
    if pid_field:
        for row in flows:
            v = (row.get(pid_field, "") or "").strip()
            if not v.isdigit():
                continue
            pid = int(v)
            flows_by_pid.setdefault(pid, []).append(row)

    # Load polling per PID
    if POLLING_DIR.exists():
        mem_path, polled_files_per_pid = detect_polled_feature_files_per_pid(POLLING_DIR)
    else:
        mem_path, polled_files_per_pid = None, {}

    mem_tl = load_polled_timeline([mem_path]) if mem_path else []

    # Load events per PID
    event_tl_per_pid = load_event_timelines_per_pid(EVENTS_DIR) if EVENTS_DIR.exists() else {}

    # Candidate PIDs from traces
    trace_pids = set(polled_files_per_pid.keys()) | set(event_tl_per_pid.keys())

    if pid_field:
        flow_pids = set(flows_by_pid.keys())
        pids = trace_pids & flow_pids
        if not pids:
            print("[join] warning: no intersection between flow PIDs and trace PIDs", file=os.sys.stderr)
            pids = trace_pids or flow_pids
    else:
        # No pid column, we’ll use all flows for every PID we have traces for
        pids = trace_pids
        if not pids:
            raise SystemExit("[error] no per-PID polling or event timelines loaded")

    for pid in sorted(pids):
        flows_for_pid = flows_by_pid.get(pid, flows if not pid_field else [])
        if not flows_for_pid:
            print(f"[join] warning: PID={pid} has no flows, skipping", file=os.sys.stderr)
            continue

        print(f"[join] processing PID={pid}", file=os.sys.stderr)

        # Build polled timelines for this PID
        polled_tl = {}
        if pid in polled_files_per_pid:
            for metric, path in polled_files_per_pid[pid].items():
                tl = load_polled_timeline([path])
                if tl:
                    polled_tl[metric] = tl

        if mem_tl:
            polled_tl["memavailable_bytes"] = mem_tl

        # Event timelines for this PID
        pid_events = event_tl_per_pid.get(pid, {})

        if not polled_tl and not pid_events:
            print(f"[join] warning: PID={pid} has no timelines, skipping", file=os.sys.stderr)
            continue

        polled_fields = sorted(polled_tl.keys())
        event_fields  = sorted(pid_events.keys())
        extra_fields  = polled_fields + [e for evt in event_fields for e in (evt, f"tt_{evt}")]

        out_path = Path(f"{OUT_PREFIX}{pid}.csv")
        with open(out_path, "w", newline="", encoding="utf-8") as fout:
            w = csv.DictWriter(fout, fieldnames=fields + extra_fields)
            w.writeheader()

            missing_polled = {k: 0 for k in polled_fields}
            missing_event_val = {k: 0 for k in event_fields}

            for row in flows_for_pid:
                row = dict(row)
                try:
                    start_ts_ns = int(row["start_ts_ns"])
                except:
                    start_ts_ns = None

                # polled features
                for feat in polled_fields:
                    if start_ts_ns is None:
                        row[feat] = ""
                        missing_polled[feat] += 1
                        continue
                    tl = polled_tl[feat]
                    if not tl:
                        row[feat] = ""
                        missing_polled[feat] += 1
                        continue
                    times = [x[0] for x in tl]
                    i = bisect_right(times, start_ts_ns) - 1
                    if i < 0:
                        row[feat] = ""
                        missing_polled[feat] += 1
                    else:
                        val = tl[i][1]
                        row[feat] = int(val) if float(val).is_integer() else f"{val:.6f}"

                # event features
                for evt in event_fields:
                    val_key, tt_key = evt, f"tt_{evt}"
                    row[val_key] = ""
                    row[tt_key] = ""
                    if start_ts_ns is None:
                        missing_event_val[evt] += 1
                        continue
                    tl = pid_events[evt]
                    pair = last_sample_at_or_before(tl, start_ts_ns)
                    if pair is None:
                        missing_event_val[evt] += 1
                    else:
                        t_e, v_e = pair
                        row[val_key] = int(v_e) if float(v_e).is_integer() else f"{v_e:.6f}"
                        dt_s = max(0, (start_ts_ns - t_e) / 1_000_000_000.0)
                        row[tt_key] = f"{dt_s:.6f}"

                w.writerow(row)

        print(f"[join] PID={pid} wrote: {out_path}", file=os.sys.stderr)
        for feat, cnt in (polled_fields and missing_polled or {}).items():
            if cnt:
                print(f"[join] PID={pid} polled {feat}: {cnt} missing", file=os.sys.stderr)
        for evt, cnt in (event_fields and missing_event_val or {}).items():
            if cnt:
                print(f"[join] PID={pid} event {evt}: {cnt} missing", file=os.sys.stderr)

if __name__ == "__main__":
    main()
