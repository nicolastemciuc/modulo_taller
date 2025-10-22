#!/usr/bin/env bash

PID_FILE="${PID_FILE:-/mnt/extradisk/workloads/latest/pids.txt}"

# Launch process in background but keep output visible
accelerate launch fine_tuning.py &
PID=$!

# Append PID to file
echo $PID >> "$PIDFILE"

echo "Process started with PID: $PID"