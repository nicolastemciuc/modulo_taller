#!/usr/bin/env bash

PID_FILE="${PID_FILE:-/mnt/extradisk/workloads/latest/pids.txt}"
REMOTE_USER="clustergpu2"
REMOTE_HOST="192.168.60.85"
REMOTE_DIR="/mnt/extra/GPT_Model/QA-LoRA-Lab"

# Launch local process
accelerate launch fine_tuning.py &
PID=$!
echo $PID >> "$PIDFILE"
echo "Local process started with PID: $PID"

# Launch remote process
ssh -i ~/.ssh/id_rsa_gpt "$REMOTE_USER@$REMOTE_HOST" "cd '$REMOTE_DIR' && nohup accelerate launch fine_tuning.py > /dev/null 2>&1 &"

echo "Remote process launched on $REMOTE_HOST in $REMOTE_DIR"