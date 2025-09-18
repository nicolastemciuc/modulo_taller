import re
import csv

# Regex to parse tcpdump line
line_re = re.compile(
    r'(?P<timestamp>\d+:\d+:\d+\.\d+)\s+IP\s+'
    r'(?P<src_ip>\d+\.\d+\.\d+\.\d+)\.(?P<src_port>\d+)\s+>\s+'
    r'(?P<dst_ip>\d+\.\d+\.\d+\.\d+)\.(?P<dst_port>\d+):.*length\s+(?P<length>\d+)'
)

def parse_tcpdump(input_file, output_file):
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        # Write CSV header
        writer.writerow(["timestamp", "source_ip", "source_port", "destination_ip", "destination_port", "packet_size"])

        for line in infile:
            match = line_re.search(line)
            if match:
                writer.writerow([
                    match.group("timestamp"),
                    match.group("src_ip"),
                    match.group("src_port"),
                    match.group("dst_ip"),
                    match.group("dst_port"),
                    match.group("length")
                ])

if __name__ == "__main__":
    parse_tcpdump("packets.log", "output.csv")

