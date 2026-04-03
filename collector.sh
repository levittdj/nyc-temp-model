#!/usr/bin/env bash
set -euo pipefail
cd ~/nyc-temp-model
source .venv/bin/activate
python collector.py >> logs/collector.log 2>&1
