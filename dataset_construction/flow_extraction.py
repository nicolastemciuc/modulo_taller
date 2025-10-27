#!/usr/bin/env python3
import csv, os
from bisect import bisect_right

FLOW_GAP_US = 5000
VANTAGE_IP = "192.168.60.81"

def cumulative_at(timeline, t_ns):
    if not timeline: return 0
    times = [x[0] for x in timeline]
    idx = bisect_right(times, t_ns) - 1
    return 0 if idx < 0 else timeline[idx][1]

def extract_flows(input_file, output_file, flow_gap_us, vantage_ip):
    flow_gap_ns = flow_gap_us * 1000
    fid = 0
    groups = {}
    events = []

    with open(input_file, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            t_ns = int(row["ts_ns"])
            ts_raw = row.get("timestamp_epoch_s","")
            src_ip, dst_ip = row["source_ip"], row["destination_ip"]
            key = (src_ip, row["source_port"], dst_ip, row["destination_port"])
            pkt = {"t_ns": t_ns, "size": int(row["packet_size"]), "ts_raw": ts_raw}
            groups.setdefault(key, []).append(pkt)
            events.append({"t_ns": t_ns, "src": src_ip, "dst": dst_ip, "size": pkt["size"]})

    events.sort(key=lambda e: e["t_ns"])
    outgoing_tl, incoming_tl, out_cum, in_cum = {}, {}, {}, {}
    for e in events:
        out_cum[e["src"]] = out_cum.get(e["src"], 0) + e["size"]
        in_cum[e["dst"]] = in_cum.get(e["dst"], 0) + e["size"]
        outgoing_tl.setdefault(e["src"], []).append((e["t_ns"], out_cum[e["src"]]))
        incoming_tl.setdefault(e["dst"], []).append((e["t_ns"], in_cum[e["dst"]]))

    with open(output_file, "w", newline="", encoding="utf-8") as fo:
        w = csv.writer(fo)
        w.writerow([
            "flow_id","src_ip","src_port","dst_ip","dst_port",
            "start_time","end_time","start_ts_ns",
            "packet_count","total_size","gap_us","neworkin","neworkout"
        ])

        for (src_ip, src_port, dst_ip, dst_port), packets in groups.items():
            packets.sort(key=lambda p: p["t_ns"])
            cur = []
            last_ns = prev_end_ns = None

            def flush(cur, prev_end_ns):
                nonlocal fid
                if not cur: return prev_end_ns
                start_ns, end_ns = cur[0]["t_ns"], cur[-1]["t_ns"]
                start_ts, end_ts = cur[0]["ts_raw"], cur[-1]["ts_raw"]
                gap_us = 0 if prev_end_ns is None else (start_ns - prev_end_ns) // 1000
                ref_ip = vantage_ip or src_ip
                net_in = cumulative_at(incoming_tl.get(ref_ip), start_ns - 1)
                net_out = cumulative_at(outgoing_tl.get(ref_ip), start_ns - 1)
                w.writerow([
                    fid, src_ip, src_port, dst_ip, dst_port,
                    start_ts, end_ts, start_ns,
                    len(cur), sum(x["size"] for x in cur),
                    gap_us, net_in, net_out
                ])
                fid += 1
                return end_ns

            for pkt in packets:
                if last_ns is not None and pkt["t_ns"] - last_ns > flow_gap_ns:
                    prev_end_ns = flush(cur, prev_end_ns)
                    cur = []
                cur.append(pkt)
                last_ns = pkt["t_ns"]

            prev_end_ns = flush(cur, prev_end_ns)

def main():
    base = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(base, "../trace_collector/packet_traces.csv")
    output_file = os.path.join(base, "flow_extraction.csv")
    extract_flows(input_file, output_file, FLOW_GAP_US, VANTAGE_IP)

if __name__ == "__main__":
    main()
