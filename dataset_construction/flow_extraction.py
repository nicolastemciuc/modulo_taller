#!/usr/bin/env python3
import csv
import argparse
import os
from bisect import bisect_right
from typing import Dict, List, Tuple, Optional

DEFAULT_FLOW_GAP_US = 5000
DEFAULT_IP = "192.168.60.81"

# Parse "HH:MM:SS.ssssss" -> float seconds
def parse_time_hms(ts: str) -> float:
    h, m, s = ts.split(":")
    return int(h) * 3600 + int(m) * 60 + float(s)

# Binary-search over [(t_ns, value), ...] to get value at or before t_ns
def cumulative_at(timeline: Optional[List[Tuple[int, int]]], t_ns: int) -> int:
    if not timeline:
        return 0
    times = [x[0] for x in timeline]
    idx = bisect_right(times, t_ns) - 1
    if idx < 0:
        return 0
    return timeline[idx][1]

def extract_flows(input_file: str, output_file: str, flow_gap_us: int, vantage_ip: Optional[str]) -> None:
    flow_gap_ns = int(flow_gap_us) * 1000
    global_flow_id = 0

    # Group packets by 4-tuple
    groups: Dict[Tuple[str, str, str, str], List[Dict]] = {}
    events: List[Dict] = []

    with open(input_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        have_ts_ns = "ts_ns" in reader.fieldnames

        for row in reader:
            if have_ts_ns:
                t_ns = int(row["ts_ns"])
                ts_raw = row.get("timestamp_epoch_s", "")  # informational only
            else:
                # fallback: derive ns from HH:MM:SS.ssssss relative time
                t_s = parse_time_hms(row["timestamp"])
                t_ns = int(t_s * 1_000_000_000)
                ts_raw = row["timestamp"]

            src_ip = row["source_ip"]
            dst_ip = row["destination_ip"]
            size = int(row["packet_size"])

            key = (src_ip, row["source_port"], dst_ip, row["destination_port"])
            groups.setdefault(key, []).append(
                {"t_ns": t_ns, "size": size, "ts_raw": ts_raw}
            )
            events.append({"t_ns": t_ns, "src": src_ip, "dst": dst_ip, "size": size})

    # Build cumulative timelines (ns) per IP
    events.sort(key=lambda e: e["t_ns"])
    outgoing_tl: Dict[str, List[Tuple[int, int]]] = {}
    incoming_tl: Dict[str, List[Tuple[int, int]]] = {}
    out_cum: Dict[str, int] = {}
    in_cum: Dict[str, int] = {}

    for e in events:
        out_cum[e["src"]] = out_cum.get(e["src"], 0) + e["size"]
        in_cum[e["dst"]] = in_cum.get(e["dst"], 0) + e["size"]
        outgoing_tl.setdefault(e["src"], []).append((e["t_ns"], out_cum[e["src"]]))
        incoming_tl.setdefault(e["dst"], []).append((e["t_ns"], in_cum[e["dst"]]))

    # Overwrite on each run
    with open(output_file, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.writer(f_out)
        writer.writerow([
            "flow_id", "src_ip", "src_port", "dst_ip", "dst_port",
            "start_time", "end_time", "start_ts_ns",  # start_ts_ns added
            "packet_count", "total_size",
            "gap_us", "neworkin", "neworkout"  # keep Ruby header names as-is
        ])

        for (src_ip, src_port, dst_ip, dst_port), packets in groups.items():
            packets.sort(key=lambda p: p["t_ns"])

            current_flow: List[Dict] = []
            last_pkt_ns: Optional[int] = None
            prev_flow_end_ns: Optional[int] = None

            for pkt in packets:
                if last_pkt_ns is not None and (pkt["t_ns"] - last_pkt_ns > flow_gap_ns):
                    # close current flow
                    start_ns = current_flow[0]["t_ns"]
                    end_ns = current_flow[-1]["t_ns"]
                    start_ts = current_flow[0]["ts_raw"]
                    end_ts = current_flow[-1]["ts_raw"]
                    gap_us = 0 if prev_flow_end_ns is None else (start_ns - prev_flow_end_ns) // 1000

                    ref_ip = vantage_ip or src_ip
                    net_in = cumulative_at(incoming_tl.get(ref_ip), start_ns - 1)
                    net_out = cumulative_at(outgoing_tl.get(ref_ip), start_ns - 1)

                    writer.writerow([
                        global_flow_id, src_ip, src_port, dst_ip, dst_port,
                        start_ts, end_ts, start_ns,
                        len(current_flow),
                        sum(x["size"] for x in current_flow),
                        gap_us, net_in, net_out
                    ])

                    global_flow_id += 1
                    prev_flow_end_ns = end_ns
                    current_flow = []

                current_flow.append(pkt)
                last_pkt_ns = pkt["t_ns"]

            # flush last flow for this 4-tuple
            if current_flow:
                start_ns = current_flow[0]["t_ns"]
                end_ns = current_flow[-1]["t_ns"]
                start_ts = current_flow[0]["ts_raw"]
                end_ts = current_flow[-1]["ts_raw"]
                gap_us = 0 if prev_flow_end_ns is None else (start_ns - prev_flow_end_ns) // 1000

                ref_ip = vantage_ip or src_ip
                net_in = cumulative_at(incoming_tl.get(ref_ip), start_ns - 1)
                net_out = cumulative_at(outgoing_tl.get(ref_ip), start_ns - 1)

                writer.writerow([
                    global_flow_id, src_ip, src_port, dst_ip, dst_port,
                    start_ts, end_ts, start_ns,
                    len(current_flow),
                    sum(x["size"] for x in current_flow),
                    gap_us, net_in, net_out
                ])
                global_flow_id += 1

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # FIXED PATH: read from ../packet_traces/packet_traces.csv
    input_file = os.path.join(script_dir, "../packet_traces/packet_traces.csv")
    output_file = os.path.join(script_dir, "flow_extraction.csv")

    parser = argparse.ArgumentParser(description="Extract flows (absolute time via ts_ns) for joining with polling traces.")
    parser.add_argument("--ip", default=DEFAULT_IP, help="Vantage IP address")
    parser.add_argument("--gap-us", type=int, default=DEFAULT_FLOW_GAP_US, help=f"Flow gap in microseconds")
    args = parser.parse_args()

    print(f"Reading: {input_file}")
    print(f"Writing: {output_file} (overwrite)")
    print(f"Vantage IP: {args.ip} | Gap: {args.gap_us}us")

    extract_flows(input_file, output_file, args.gap_us, args.ip)

if __name__ == "__main__":
    main()
