"""Shared KNYC METAR DB vs NOAA freshness check (used by test + Paul)."""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from collector import _fetch_knyc_metar_observations  # noqa: E402
from logger import latest_metar_observation_ts_utc  # noqa: E402


def z_ts(ts: datetime) -> str:
    ts = ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_metar_freshness(
    python_db: str | Path,
    station: str = "KNYC",
    lag_warn_minutes: int = 35,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    db = Path(python_db)
    best: datetime | None = None
    for i in range(4):
        t = latest_metar_observation_ts_utc(db, now.date() - timedelta(days=i), station)
        if t is not None and (best is None or t > best):
            best = t
    if best is None:
        raise RuntimeError(f"No {station} METAR in DB (checked 4 UTC days).")
    conn = sqlite3.connect(str(db))
    try:
        row = conn.execute(
            "SELECT observation_ts, tmpf, wind_speed_kt, sky_cover FROM metar_observations WHERE station=? AND observation_ts=?",
            (station, z_ts(best)),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        raise RuntimeError("DB row missing for latest observation_ts.")
    ts_s, tf, wk, sk = row[0], row[1], row[2], row[3]
    db_o = datetime.fromisoformat(str(ts_s).replace("Z", "+00:00"))
    if db_o.tzinfo is None:
        db_o = db_o.replace(tzinfo=timezone.utc)
    noaa = _fetch_knyc_metar_observations(now - timedelta(hours=3), now)
    if not noaa:
        raise RuntimeError("NOAA returned no rows for last 3 hours.")
    noaa.sort(key=lambda r: r["observation_ts"])
    lat = noaa[-1]
    lag_m = (lat["observation_ts"] - db_o).total_seconds() / 60.0
    behind = max(0.0, lag_m)
    stale = behind >= lag_warn_minutes
    return {
        "now": now,
        "db_obs": db_o,
        "db_tmpf": tf,
        "db_wind_kt": wk,
        "db_sky": sk,
        "noaa_latest": lat,
        "noaa_rows": noaa,
        "lag_minutes": lag_m,
        "stale": stale,
        "exit_code": 0 if not stale else 1,
    }
