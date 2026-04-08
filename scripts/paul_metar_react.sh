#!/usr/bin/env bash
set -euo pipefail
cd /home/ubuntu/nyc-temp-model
source .venv/bin/activate
python scripts/paul_metar_react.py
