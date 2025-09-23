#!/bin/zsh
set -e

IFACE="ens3"
CAPTURE_FILE="packets.log"

echo "[*] Capturing packets on $IFACE"
sudo tcpdump_pfring -ni "$IFACE" -l 'tcp[13] & 0x17 != 0x10' | tee "$CAPTURE_FILE"
