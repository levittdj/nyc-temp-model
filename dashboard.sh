#!/usr/bin/env bash
set -euo pipefail
cd ~/nyc-temp-model
source .venv/bin/activate
streamlit run dashboard/app.py \
  --server.port 8501 \
  --server.address 0.0.0.0 \
  >> logs/dashboard.log 2>&1
