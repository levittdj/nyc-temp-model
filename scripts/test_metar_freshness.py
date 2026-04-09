#!/usr/bin/env python3
"""Compare latest KNYC METAR in SQLite vs IEM (last 3 h). Run from repo root."""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
ROOT = SCRIPTS.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPTS))
from metar_freshness_common import run_metar_freshness, z_ts  # noqa: E402

pythonDB = "/home/ubuntu/nyc-temp-model/nyc_temp_log.sqlite"
STATION = "KNYC"
LAG_WARN_MINUTES = 35  # PROVISIONAL


def _full(ts, tmpf, wkt, sky):
    t, w, s = (f"{tmpf:.1f}" if tmpf is not None else "—"), (f"{wkt}kt" if wkt is not None else "—"), sky or "—"
    return f"{z_ts(ts)}  tmpf={t}  wind={w}  sky={s}"


def main() -> int:
    r = run_metar_freshness(pythonDB, STATION, LAG_WARN_MINUTES)
    now, db_o = r["now"], r["db_obs"]
    lat, noaa, lag_m = r["noaa_latest"], r["noaa_rows"], r["lag_minutes"]
    if lag_m > 0.5:
        lag = f"LAG: {round(lag_m):.0f} min — DB is stale. Collector may have missed cycles."
    elif lag_m < -0.5:
        lag = f"LAG: {round(-lag_m):.0f} min — DB ahead of NOAA latest (clock/window?)."
    else:
        lag = "LAG: ~0 min — DB matches NOAA latest."
    print("=== METAR Freshness Check ===\nNow (UTC):      " + z_ts(now) + "\n")
    print(f"DB latest:      {_full(db_o, r['db_tmpf'], r['db_wind_kt'], r['db_sky'])}")
    print(f"NOAA latest:    {_full(lat['observation_ts'], lat.get('tmpf'), lat.get('wind_speed_kt'), lat.get('sky_cover'))}")
    print("\n" + lag + "\n\nNOAA last 3 hours:")
    for row in noaa:
        v = row.get("tmpf")
        print(f"  {z_ts(row['observation_ts'])}  tmpf={v:.1f}" if v is not None else f"  {z_ts(row['observation_ts'])}  tmpf=—")
    return int(r["exit_code"])


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise SystemExit(1)
