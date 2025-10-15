#!/usr/bin/env python3
import csv
import re

# Regex to parse tcpdump line
LINE_RE = re.compile(
    r'(?P<timestamp>\d+:\d+:\d+\.\d+)\s+IP\s+'
    r'(?P<src_ip>\d+\.\d+\.\d+\.\d+)\.(?P<src_port>\d+)\s+>\s+'
    r'(?P<dst_ip>\d+\.\d+\.\d+\.\d+)\.(?P<dst_port>\d+):.*length\s+(?P<length>\d+)'
)

def parse_tcpdump(input_file, output_file):
    with open(output_file, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        # Write CSV header
        writer.writerow([
            "timestamp", "source_ip", "source_port",
            "destination_ip", "destination_port", "packet_size"
        ])

        with open(input_file, "r") as infile:
            for line in infile:
                match = LINE_RE.search(line)
                if match:
                    writer.writerow([
                        match.group("timestamp"),
                        match.group("src_ip"),
                        match.group("src_port"),
                        match.group("dst_ip"),
                        match.group("dst_port"),
                        match.group("length"),
                    ])

if __name__ == "__main__":
    parse_tcpdump("packets.log", "packets.csv")
