#!/usr/bin/env bash
set -euo pipefail
cd ~/nyc-temp-model
source .venv/bin/activate
python logger.py $(date -d yesterday +%Y-%m-%d) >> logs/backfill.log 2>&1
