#!/usr/bin/env bash
set -euo pipefail
cd ~/nyc-temp-model
source .venv/bin/activate
python morning_model.py >> logs/morning_model.log 2>&1
