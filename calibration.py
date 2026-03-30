#!/usr/bin/env python3
"""
One-time calibration: estimate nbm_bias for KNYC v0.

Pulls KNYC ASOS hourly temperatures (Iowa Environmental Mesonet) and ERA5-land
daily 2m max via Open-Meteo archive (NBM p50 proxy). Aligns on America/New_York
calendar days, then sets nbm_bias to the OLS intercept in

    (ASOS_max - proxy_p50) ~ 1

which equals mean(ASOS_max - proxy_p50). Prints a full ASOS ~ proxy OLS line
for diagnostics. See project_docs.html (ERA5 proxy caveat).
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:  # Python < 3.9
    from backports.zoneinfo import ZoneInfo  # type: ignore

# --- PROVISIONAL: sanity gate on |nbm_bias| before writing config (starting estimate, not validated).
NBM_BIAS_WARN_ABS_F = 3.0

ASOS_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"

# Central Park / KNYC (ASOS)
KNYC_LAT = 40.7789
KNYC_LON = -73.9692
STATION = "KNYC"
TZ_NAME = "America/New_York"


@dataclass(frozen=True)
class DailyPair:
    """One calendar day with observed max and NBM p50 proxy (°F)."""

    d: date
    asos_max_f: float
    proxy_p50_f: float


def _zone():
    return ZoneInfo(TZ_NAME)


def fetch_asos_hourly_tmpf(sts_utc: datetime, ets_utc: datetime) -> list[tuple[datetime, float]]:
    """
    Download KNYC ASOS hourly tmpf between sts_utc and ets_utc (timezone-aware UTC).

    Returns list of (valid UTC datetime, tmpf).
    """
    params = {
        "station": STATION,
        "data": "tmpf",
        "sts": sts_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ets": ets_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tz": TZ_NAME,
        "format": "onlycomma",
    }
    url = f"{ASOS_URL}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": "nyc-temp-model-calibration/0"})
    with urlopen(req, timeout=120) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    if raw.lstrip().startswith("["):
        raise RuntimeError(f"Iowa Mesonet returned an error: {raw[:500]}")
    rows = []
    reader = csv.DictReader(StringIO(raw))
    for rec in reader:
        tstr = rec.get("valid") or rec.get("valid(UTC)") or ""
        v = rec.get("tmpf", "M").strip()
        if v in ("M", "", "null"):
            continue
        try:
            tmpf = float(v)
        except ValueError:
            continue
        # Mesonet returns valid in requested tz when tz= is set
        local = datetime.strptime(tstr, "%Y-%m-%d %H:%M").replace(tzinfo=_zone())
        rows.append((local.astimezone(timezone.utc), tmpf))
    return rows


def daily_max_from_hourly(
    hourly: list[tuple[datetime, float]],
) -> dict[date, float]:
    """Bucket into America/New_York calendar dates; take max tmpf per day."""
    z = _zone()
    by_day: dict[date, list[float]] = defaultdict(list)
    for utc_dt, tmpf in hourly:
        local = utc_dt.astimezone(z)
        by_day[local.date()].append(tmpf)
    return {d: max(vals) for d, vals in by_day.items() if vals}


def fetch_open_meteo_era5_max_f(start: date, end: date) -> dict[date, float]:
    """
    Daily 2m max temperature (ERA5) as NBM p50 proxy; returns °F per local date.
    """
    params = {
        "latitude": KNYC_LAT,
        "longitude": KNYC_LON,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": "temperature_2m_max",
        "timezone": TZ_NAME,
    }
    url = f"{OPEN_METEO_ARCHIVE}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": "nyc-temp-model-calibration/0"})
    with urlopen(req, timeout=60) as resp:
        payload = json.loads(resp.read().decode())
    if "daily" not in payload:
        raise RuntimeError(f"Open-Meteo error: {payload}")
    times = payload["daily"]["time"]
    cmax = payload["daily"]["temperature_2m_max"]
    out: dict[date, float] = {}
    for t, tc in zip(times, cmax):
        if tc is None:
            continue
        d = date.fromisoformat(t)
        tf = float(tc) * 9.0 / 5.0 + 32.0
        out[d] = tf
    return out


def ols_simple(xs: list[float], ys: list[float]) -> tuple[float, float, float]:
    """
    OLS fit y ~ a + b*x. Returns (intercept, slope, r_squared).
    """
    n = len(xs)
    if n != len(ys) or n < 2:
        raise ValueError("Need at least two paired points for OLS.")
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx == 0:
        raise ValueError("Zero variance in x.")
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    b = sxy / sxx
    a = my - b * mx
    ss_res = sum((y - (a + b * x)) ** 2 for x, y in zip(xs, ys))
    sst = sum((y - my) ** 2 for y in ys)
    r2 = 1.0 - ss_res / sst if sst > 0 else 0.0
    return a, b, r2


def build_pairs(
    asos_daily: dict[date, float], proxy_daily: dict[date, float]
) -> list[DailyPair]:
    """Inner join on calendar date."""
    keys = sorted(set(asos_daily) & set(proxy_daily))
    return [
        DailyPair(d=k, asos_max_f=asos_daily[k], proxy_p50_f=proxy_daily[k])
        for k in keys
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Calibrate nbm_bias (KNYC v0).")
    parser.add_argument(
        "--years",
        type=int,
        default=3,
        help="Lookback in years (default 3).",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).resolve().parent / "config.json",
        help="Path to config.json to write.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and print but do not write config.json.",
    )
    parser.add_argument(
        "--accept-large-bias",
        action="store_true",
        help="Exit 0 even if |nbm_bias| >= 3F (PROVISIONAL gate).",
    )
    args = parser.parse_args()

    z = _zone()
    end_local = datetime.now(z).date() - timedelta(days=1)
    start_local = end_local - timedelta(days=365 * args.years)

    sts_utc = datetime.combine(start_local, datetime.min.time(), tzinfo=z).astimezone(
        timezone.utc
    )
    ets_utc = (
        datetime.combine(end_local + timedelta(days=1), datetime.min.time(), tzinfo=z)
        .astimezone(timezone.utc)
    )

    print("Fetching ASOS hourly tmpf (Iowa Mesonet)...", flush=True)
    hourly = fetch_asos_hourly_tmpf(sts_utc, ets_utc)
    if not hourly:
        raise SystemExit("No ASOS rows returned; check station and date range.")
    asos_daily = daily_max_from_hourly(hourly)

    print("Fetching ERA5 daily max via Open-Meteo (NBM p50 proxy)...", flush=True)
    proxy_daily = fetch_open_meteo_era5_max_f(start_local, end_local)

    pairs = build_pairs(asos_daily, proxy_daily)
    if len(pairs) < 30:
        raise SystemExit(
            f"Too few overlapping days ({len(pairs)}); check data pulls and dates."
        )

    resid = [p.asos_max_f - p.proxy_p50_f for p in pairs]
    nbm_bias = sum(resid) / len(resid)

    xs = [p.proxy_p50_f for p in pairs]
    ys = [p.asos_max_f for p in pairs]
    a_full, b_full, r2 = ols_simple(xs, ys)

    print(f"Overlapping days: {len(pairs)}")
    print(f"Date range (local): {pairs[0].d} .. {pairs[-1].d}")
    print(f"OLS ASOS_max ~ a + b * proxy_p50: a={a_full:.3f}F, b={b_full:.4f}, R^2={r2:.4f}")
    print(f"nbm_bias = mean(ASOS_max - proxy_p50) = {nbm_bias:.4f}F")

    if abs(nbm_bias) >= NBM_BIAS_WARN_ABS_F:
        print(
            f"WARNING: |nbm_bias| >= {NBM_BIAS_WARN_ABS_F}F (PROVISIONAL gate). "
            "Investigate data quality / proxy adequacy before trusting.",
            file=sys.stderr,
        )
        if not args.accept_large_bias:
            print("Refusing to write config (use --accept-large-bias to override).", file=sys.stderr)
            if not args.dry_run:
                return 1

    if args.dry_run:
        print("Dry run: not writing config.json.")
        return 0

    args.config.write_text(
        json.dumps({"nbm_bias": round(nbm_bias, 4)}, indent=2) + "\n", encoding="utf-8"
    )
    print(f"Wrote {args.config}")
    return 0 if args.accept_large_bias or abs(nbm_bias) < NBM_BIAS_WARN_ABS_F else 1


if __name__ == "__main__":
    raise SystemExit(main())
