#!/usr/bin/env python3
import csv
import argparse
import os
from bisect import bisect_right
from typing import Dict, List, Tuple, Optional

# Flow gaps in microseconds
FLOW_GAPS = [500, 5000, 10000]
DEFAULT_IP = "192.168.60.81"  # mirrors Ruby's DEFAULT_IP

# Parse "HH:MM:SS.ssssss" -> float seconds
def parse_time(ts: str) -> float:
    h, m, s = ts.split(":")
    return int(h) * 3600 + int(m) * 60 + float(s)

# Binary-search helper: given a sorted timeline [[t0, v0], [t1, v1], ...],
# return cumulative value at time <= t (or 0 if none).
def cumulative_at(timeline: Optional[List[Tuple[float, int]]], t: float) -> int:
    if not timeline:
        return 0
    # timeline sorted by time; find rightmost index with time <= t
    times = [x[0] for x in timeline]
    idx = bisect_right(times, t) - 1
    if idx < 0:
        return 0
    return timeline[idx][1]

def extract_flows(input_file: str, output_file: str, flow_gap_us: int, vantage_ip: Optional[str]) -> None:
    flow_gap_s = float(flow_gap_us) / 1_000_000.0
    global_flow_id = 0

    # Group packets by 4-tuple
    groups: Dict[Tuple[str, str, str, str], List[Dict]] = {}
    events: List[Dict] = []

    with open(input_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            t = parse_time(row["timestamp"])
            src_ip = row["source_ip"]
            dst_ip = row["destination_ip"]
            size = int(row["packet_size"])

            key = (src_ip, row["source_port"], dst_ip, row["destination_port"])
            groups.setdefault(key, []).append(
                {"time": t, "size": size, "ts_raw": row["timestamp"]}
            )

            events.append({"t": t, "src": src_ip, "dst": dst_ip, "size": size})

    # Build cumulative incoming/outgoing timelines per IP
    events.sort(key=lambda e: e["t"])
    outgoing_tl: Dict[str, List[Tuple[float, int]]] = {}
    incoming_tl: Dict[str, List[Tuple[float, int]]] = {}
    out_cum: Dict[str, int] = {}
    in_cum: Dict[str, int] = {}

    for e in events:
        out_cum[e["src"]] = out_cum.get(e["src"], 0) + e["size"]
        in_cum[e["dst"]] = in_cum.get(e["dst"], 0) + e["size"]

        outgoing_tl.setdefault(e["src"], []).append((e["t"], out_cum[e["src"]]))
        incoming_tl.setdefault(e["dst"], []).append((e["t"], in_cum[e["dst"]]))

    with open(output_file, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.writer(f_out)
        writer.writerow([
            "flow_id", "src_ip", "src_port", "dst_ip", "dst_port",
            "start_time", "end_time", "packet_count", "total_size",
            "gap_us", "neworkin", "neworkout"  # keep Ruby header names as-is
        ])

        for (src_ip, src_port, dst_ip, dst_port), packets in groups.items():
            packets.sort(key=lambda p: p["time"])

            current_flow: List[Dict] = []
            last_pkt_time: Optional[float] = None
            prev_flow_end: Optional[float] = None

            for pkt in packets:
                if last_pkt_time is not None and (pkt["time"] - last_pkt_time > flow_gap_s):
                    # Close current flow
                    start_t = current_flow[0]["time"]
                    end_t = current_flow[-1]["time"]
                    start_ts = current_flow[0]["ts_raw"]
                    end_ts = current_flow[-1]["ts_raw"]
                    gap_us = 0 if prev_flow_end is None else round((start_t - prev_flow_end) * 1_000_000)

                    # Determine the reference IP
                    ref_ip = vantage_ip or src_ip
                    net_in = cumulative_at(incoming_tl.get(ref_ip), start_t - 1e-12)
                    net_out = cumulative_at(outgoing_tl.get(ref_ip), start_t - 1e-12)

                    writer.writerow([
                        global_flow_id, src_ip, src_port, dst_ip, dst_port,
                        start_ts, end_ts, len(current_flow),
                        sum(x["size"] for x in current_flow),
                        gap_us, net_in, net_out
                    ])

                    global_flow_id += 1
                    prev_flow_end = end_t
                    current_flow = []

                current_flow.append(pkt)
                last_pkt_time = pkt["time"]

            # Flush last flow for this 4-tuple
            if current_flow:
                start_t = current_flow[0]["time"]
                end_t = current_flow[-1]["time"]
                start_ts = current_flow[0]["ts_raw"]
                end_ts = current_flow[-1]["ts_raw"]
                gap_us = 0 if prev_flow_end is None else round((start_t - prev_flow_end) * 1_000_000)

                ref_ip = vantage_ip or src_ip
                net_in = cumulative_at(incoming_tl.get(ref_ip), start_t - 1e-12)
                net_out = cumulative_at(outgoing_tl.get(ref_ip), start_t - 1e-12)

                writer.writerow([
                    global_flow_id, src_ip, src_port, dst_ip, dst_port,
                    start_ts, end_ts, len(current_flow),
                    sum(x["size"] for x in current_flow),
                    gap_us, net_in, net_out
                ])
                global_flow_id += 1

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(script_dir, "../trace_collector/packet_traces/packets.csv")

    parser = argparse.ArgumentParser(description="Extract flows from packet CSV.")
    parser.add_argument("--ip", help="Specific vantage IP address (defaults to DEFAULT_IP)", default=DEFAULT_IP)
    args = parser.parse_args()

    vantage_ip = args.ip
    print(f"Using vantage IP: {vantage_ip or '(per-flow source_ip)'}")

    for gap in FLOW_GAPS:
        output_file = os.path.join(script_dir, f"flows_{gap}.csv")
        extract_flows(input_file, output_file, gap, vantage_ip)
        print(f"Generated {output_file}")

if __name__ == "__main__":
    main()
