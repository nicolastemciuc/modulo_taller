#!/bin/zsh
set -e

IFACE="ens3"
CAPTURE_FILE="packets.log"
CSV_FILE="output.csv"
DURATION="10s"   # capture duration

echo "[*] Capturing packets on $IFACE for $DURATION..."
sudo timeout "$DURATION" tcpdump_pfring -ni "$IFACE" -l 'tcp[13] & 0x17 != 0x10' | tee "$CAPTURE_FILE"

echo "[*] Parsing packets into CSV..."
python3 output_to_csv.py "$CAPTURE_FILE" "$CSV_FILE"

echo "[*] Done. Results saved to $CSV_FILE"
