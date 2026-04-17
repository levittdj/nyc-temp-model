#!/usr/bin/env bash
set -euo pipefail
cd ~/nyc-temp-model
source .venv/bin/activate
export PYTHONPATH="$HOME/nyc-temp-model:${PYTHONPATH:-}"
mkdir -p logs
nohup streamlit run dashboard/app.py \
  --server.port 8501 \
  --server.address 0.0.0.0 \
  --server.headless true \
  > logs/dashboard.log 2>&1 &
disown
echo "Dashboard started (PID $!). Logs: logs/dashboard.log"
