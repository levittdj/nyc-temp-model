#!/usr/bin/env python3
"""
On-demand METAR freshness → Telegram (manual / OpenClaw: run on VM).
Same check as test_metar_freshness.py; no cron.
"""
from __future__ import annotations

import sys
from pathlib import Path

import requests

SCRIPTS = Path(__file__).resolve().parent
ROOT = SCRIPTS.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPTS))
from metar_freshness_common import run_metar_freshness, z_ts  # noqa: E402

pythonDB = '/home/ubuntu/nyc-temp-model/nyc_temp_log.sqlite'
BOT_TOKEN = '8652874695:AAFie5ef1mj7YXFeCs1yFiDqOEO4A76Ekg4'
CHAT_ID = -5229782521
LAG_WARN_MINUTES = 35  # PROVISIONAL
STATION = 'KNYC'


def _tf(x: float | None) -> str:
    return f"{x:.1f}" if x is not None else "—"


def _send(text: str) -> None:
    r = requests.post(
        "https://api.telegram.org/bot" + BOT_TOKEN + "/sendMessage",
        json={"chat_id": CHAT_ID, "text": text},
        timeout=60,
    )
    r.raise_for_status()


def main() -> int:
    try:
        out = run_metar_freshness(pythonDB, STATION, LAG_WARN_MINUTES)
    except Exception as e:
        try:
            _send("METAR freshness — ERROR\n\n" + str(e))
        except Exception:
            print("METAR check failed and Telegram send failed: " + str(e), file=sys.stderr)
        return 1

    now = out["now"]
    lat = out["iem_latest"]
    lag_m = out["lag_minutes"]
    lag_disp = round(max(0.0, lag_m))
    flag = "STALE" if out["stale"] else "OK"

    lines = [
        "METAR freshness — " + z_ts(now),
        "",
        "DB:  " + z_ts(out["db_obs"]) + "  " + _tf(out["db_tmpf"]) + "°F",
        "IEM: " + z_ts(lat["observation_ts"]) + "  " + _tf(lat.get("tmpf")) + "°F",
        "Lag: " + str(lag_disp) + " min " + flag,
        "",
        "IEM last 3 hours:",
    ]
    for row in out["iem_rows"]:
        lines.append("  " + z_ts(row["observation_ts"]) + "  " + _tf(row.get("tmpf")) + "°F")

    try:
        _send("\n".join(lines))
    except Exception as e:
        print("Telegram send failed: " + str(e), file=sys.stderr)
        return 1
    return int(out["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
