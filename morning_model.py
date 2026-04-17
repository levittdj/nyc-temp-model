#!/usr/bin/env python3
"""
v0 morning model: NBM PctMaxT (p10/p50/p90) at KNYC, asymmetric zone CDF,
Kalshi KXHIGHNY mid-market prices, edge = model_prob - market_price.

Live NBM: NOAA NOMADS **NBP text** (blend_nbptx.tXXz). Station block **KNYC**
provides TXNP1 / TXNP5 / TXNP9 — operational NBM probabilistic daily max-T
deciles (10th / 50th / 90th percentiles). Parsed as plain text; no GRIB/ecCodes.

Forecast column is chosen by matching NBM valid time (cycle + FHR) to the
NWS grid maxTemperature valid start for the target local date.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
import re
import sqlite3
import statistics
import sys
import tempfile
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

TZ_NAME = "America/New_York"
KNYC_LAT = 40.7789
KNYC_LON = -73.9692
NOMADS_BLEND_PROD = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod"
NWS_USER_AGENT = "(nyc-temp-model, local)"
DEFAULT_KALSHI_SERIES = "KXHIGHNY"
NBP_STATION = "KNYC"

# PROVISIONAL: record proximity flag threshold (°F); not validated.
RECORD_PROXIMITY_F = 3.0


def _zone() -> ZoneInfo:
    return ZoneInfo(TZ_NAME)


@dataclass(frozen=True)
class ZoneModel:
    """Piecewise-linear CDF knots in °F after bias; tails mirrored from inner zones."""

    L: float
    p10: float
    p50: float
    p90: float
    U: float
    p25: Optional[float] = None
    p75: Optional[float] = None


@dataclass
class BracketRow:
    """One Kalshi bracket row for logging."""

    ticker: str
    title: str
    bracket_label: str
    lower_f: float
    upper_f: float
    market_price: float
    model_prob: float
    edge: float
    market_bid: Optional[float] = None
    market_ask: Optional[float] = None
    model_prob_triplet_cdf: Optional[float] = None  # legacy 3-knot CDF prob; A/B comparison vs primary 5-knot model_prob. NULL when p25/p75 unavailable (both CDFs identical).


def load_config(path: Path) -> float:
    """Load nbm_bias (°F) from config.json."""
    data = json.loads(path.read_text(encoding="utf-8"))
    if "nbm_bias" not in data:
        raise KeyError("config.json must contain nbm_bias")
    return float(data["nbm_bias"])


def build_zones(
    p10: float,
    p50: float,
    p90: float,
    p25: Optional[float] = None,
    p75: Optional[float] = None,
) -> ZoneModel:
    """
    Build asymmetric zone CDF support from biased percentiles.

    With optional p25/p75: seven interior knots (5 inner segments) plus tails.
    Tail mirroring: L = 2*p10 - p25, U = 2*p90 - p75 when quartiles present;
    else L = 2*p10 - p50, U = 2*p90 - p50 (legacy 3-knot).
    """
    if p25 is not None and p75 is not None:
        L = 2.0 * p10 - p25
        U = 2.0 * p90 - p75
        if L < p10 < p25 < p50 < p75 < p90 < U:
            return ZoneModel(L=L, p10=p10, p50=p50, p90=p90, U=U, p25=p25, p75=p75)
    L = 2.0 * p10 - p50
    U = 2.0 * p90 - p50
    if not (L < p10 < p50 < p90 < U):
        raise ValueError(f"Invalid zone order after bias: L={L} p10={p10} p50={p50} p90={p90} U={U}")
    return ZoneModel(L=L, p10=p10, p50=p50, p90=p90, U=U, p25=None, p75=None)


def zone_cdf(z: ZoneModel, x: float) -> float:
    """Evaluate piecewise-linear CDF at x (°F)."""
    if math.isinf(x) and x < 0:
        return 0.0
    if math.isinf(x) and x > 0:
        return 1.0
    if x <= z.L:
        return 0.0
    if x >= z.U:
        return 1.0
    if z.p25 is not None and z.p75 is not None:
        if x <= z.p10:
            t = (x - z.L) / (z.p10 - z.L)
            return t * 0.1
        if x <= z.p25:
            t = (x - z.p10) / (z.p25 - z.p10)
            return 0.1 + t * 0.15
        if x <= z.p50:
            t = (x - z.p25) / (z.p50 - z.p25)
            return 0.25 + t * 0.25
        if x <= z.p75:
            t = (x - z.p50) / (z.p75 - z.p50)
            return 0.5 + t * 0.25
        if x <= z.p90:
            t = (x - z.p75) / (z.p90 - z.p75)
            return 0.75 + t * 0.15
        t = (x - z.p90) / (z.U - z.p90)
        return 0.9 + t * 0.1
    if x <= z.p10:
        t = (x - z.L) / (z.p10 - z.L)
        return t * 0.1
    if x <= z.p50:
        t = (x - z.p10) / (z.p50 - z.p10)
        return 0.1 + t * 0.4
    if x <= z.p90:
        t = (x - z.p50) / (z.p90 - z.p50)
        return 0.5 + t * 0.4
    t = (x - z.p90) / (z.U - z.p90)
    return 0.9 + t * 0.1


def bracket_prob(z: ZoneModel, lower_f: float, upper_f: float) -> float:
    """Probability mass on (lower_f, upper_f) using CDF difference; open tails use ±inf."""
    return zone_cdf(z, upper_f) - zone_cdf(z, lower_f)


def apply_nbm_bias(p10: float, p50: float, p90: float, bias_f: float) -> tuple[float, float, float]:
    """Shift all NBM percentiles by nbm_bias (°F)."""
    return p10 + bias_f, p50 + bias_f, p90 + bias_f


def _http_json(url: str) -> Any:
    req = Request(url, headers={"User-Agent": NWS_USER_AGENT, "Accept": "application/json"})
    with urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())


def _http_text(url: str, timeout: float = 120) -> str:
    req = Request(url, headers={"User-Agent": NWS_USER_AGENT})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read().decode()


def list_blend_cycle_hours(blend_ymd: str) -> list[int]:
    """Parse NOMADS directory listing for blend.yyyymmdd — available UTC cycle folders (00–23)."""
    index_url = f"{NOMADS_BLEND_PROD}/blend.{blend_ymd}/"
    try:
        html = _http_text(index_url, timeout=60)
    except (HTTPError, URLError, TimeoutError, OSError):
        return []
    found = {int(m.group(1)) for m in re.finditer(r'href="(\d{2})/"', html)}
    return sorted(found, reverse=True)


def nws_grid_url_for_point(lat: float, lon: float) -> str:
    props = _http_json(f"https://api.weather.gov/points/{lat},{lon}")["properties"]
    return str(props["forecastGridData"])


def parse_iso_duration_start(valid_time: str) -> datetime:
    """Parse '2026-03-30T12:00:00+00:00/PT13H' -> start as aware UTC."""
    start_part = valid_time.split("/")[0]
    return datetime.fromisoformat(start_part.replace("Z", "+00:00"))


def nws_daily_max_period_for_date(grid_url: str, target: date) -> tuple[datetime, float]:
    """
    Find maxTemperature grid cell whose validity *start* falls on target in America/New_York.

    Returns (valid_start_utc, value_degF).
    """
    grid = _http_json(grid_url)
    mt = grid["properties"]["maxTemperature"]
    uom = mt.get("uom", "")
    z = _zone()
    for cell in mt["values"]:
        vt = cell.get("validTime")
        val = cell.get("value")
        if vt is None or val is None:
            continue
        start_utc = parse_iso_duration_start(vt)
        if start_utc.astimezone(z).date() == target:
            c = float(val)
            if "degC" in uom or "Cel" in uom:
                f = c * 9.0 / 5.0 + 32.0
            else:
                f = c
            return start_utc, f
    raise RuntimeError(f"No NWS maxTemperature grid cell for local date {target}")


def nbp_text_url(blend_ymd: str, cycle_hh: int) -> str:
    return (
        f"{NOMADS_BLEND_PROD}/blend.{blend_ymd}/{cycle_hh:02d}/text/"
        f"blend_nbptx.t{cycle_hh:02d}z"
    )


def stream_extract_station_block(url: str, station: str) -> Optional[list[str]]:
    """
    Read blend_nbptx from URL line-by-line; stop after KNYC block (avoids full 33MB when possible).
    Returns None on 404 / empty / missing block.
    """
    req = Request(url, headers={"User-Agent": NWS_USER_AGENT})
    try:
        resp = urlopen(req, timeout=180)
    except HTTPError as e:
        if getattr(e, "code", None) == 404:
            return None
        raise
    try:
        if resp.status != 200:
            return None
        lines: list[str] = []
        inside = False
        start_re = re.compile(rf"^\s*{re.escape(station)}\s+NBM\s+V")
        next_re = re.compile(r"^\s*[A-Z0-9]+\s+NBM\s+V")
        while True:
            raw = resp.readline()
            if not raw:
                break
            line = raw.decode("utf-8", errors="replace")
            if not inside:
                if start_re.match(line):
                    inside = True
                    lines.append(line)
                continue
            if next_re.match(line) and station not in line[:12]:
                break
            lines.append(line)
        return lines if len(lines) >= 4 else None
    finally:
        resp.close()


def parse_nbp_numeric_row(line: str) -> Optional[tuple[str, list[Optional[int]]]]:
    """
    Parse one NBP data row into key and flat list of integers per column (| groups flattened).
    Missing cells become None.
    """
    line = line.rstrip()
    m = re.match(r"^\s*([A-Z][A-Z0-9]{2,6})\s+(.*)$", line)
    if not m:
        return None
    key, rest = m.group(1), m.group(2)
    cols: list[Optional[int]] = []
    for part in rest.split("|"):
        toks = part.split()
        if not toks:
            continue
        for tok in toks:
            try:
                cols.append(int(tok))
            except ValueError:
                cols.append(None)
    return key, cols


def parse_nbptx_station(block_lines: list[str]) -> dict[str, list[Optional[int]]]:
    """Build map of row key -> integer columns for one station block."""
    rows: dict[str, list[Optional[int]]] = {}
    for line in block_lines[1:]:
        parsed = parse_nbp_numeric_row(line)
        if parsed:
            rows[parsed[0]] = parsed[1]
    return rows


def column_valid_times_utc(cycle_init: datetime, fhr_row: list[Optional[int]]) -> list[Optional[datetime]]:
    out: list[Optional[datetime]] = []
    for fh in fhr_row:
        if fh is None:
            out.append(None)
        else:
            out.append(cycle_init + timedelta(hours=int(fh)))
    return out


def _nbp_cell_float(rows: dict[str, Any], key: str, j: int) -> Optional[float]:
    if key not in rows:
        return None
    row = rows[key]
    if j >= len(row):
        return None
    v = row[j]
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def pick_column_index(
    valid_times: list[Optional[datetime]],
    fhr_row: list[Optional[int]],
    target_date: date,
) -> Optional[int]:
    """
    Column whose valid UTC falls on target_date in America/New_York; FHR in [6, 48].
    NBM NBP text uses 12-hour periods. The daytime max period ends at ~00Z the
    following UTC day (8pm ET). Prefer the column whose valid_time is closest to
    midnight UTC on target_date + 1 — this selects the daytime period (8am-8pm ET)
    over the overnight period (8pm-8am ET).
    """
    z = _zone()
    d1 = target_date + timedelta(days=1)
    anchor = datetime(d1.year, d1.month, d1.day, 0, 0, tzinfo=timezone.utc)
    best_j: Optional[int] = None
    best_dist: Optional[float] = None
    for j, vt in enumerate(valid_times):
        if vt is None:
            continue
        fh = fhr_row[j] if j < len(fhr_row) else None
        if fh is None or fh < 6 or fh > 48:
            continue
        if vt.astimezone(z).date() != target_date:
            continue
        dist = abs((vt - anchor).total_seconds())
        if best_j is None or dist < best_dist:
            best_j = j
            best_dist = dist
    return best_j


def fetch_pctmax_from_nbp_text(
    station: str,
    valid_start_utc: datetime,
    nws_p50_f: float,
) -> tuple[float, float, float, dict[str, Any]]:
    """
    Download latest-available NBP bulletin; extract TXNP1/TXNP5/TXNP9 (°F) at best FHR column.

    TXNP* rows: NBM probabilistic daily max-T deciles in °F (TXNP1/5/9 required; TXNP2 p25,
    TXNP7 p75, TXNSD stdev optional at the same column).
    Column selection prefers valid time closest to 00Z UTC on the day after the target
    local date (end of NBM daytime max window, ~8pm ET).

    Returns (p10, p50, p90, meta). meta may include nbm_p25_raw, nbm_p75_raw, nbm_sd_raw.
    """
    target_date = valid_start_utc.astimezone(_zone()).date()
    anchor_date = target_date
    for day_off in (0, -1, -2):
        d = anchor_date + timedelta(days=day_off)
        ymd = d.strftime("%Y%m%d")
        hours = list_blend_cycle_hours(ymd)
        if not hours:
            continue
        preferred = {1, 7, 13, 19}
        hours_ord = sorted(hours, key=lambda h: (0 if h in preferred else 1, -h))
        # Prefer MDL ops-text cycles (01/07/13/19Z); then newest. Some hours omit NBP data rows for KNYC.
        for hh in hours_ord[:12]:
            url = nbp_text_url(ymd, hh)
            try:
                block = stream_extract_station_block(url, station)
            except (HTTPError, URLError, TimeoutError, OSError):
                continue
            if not block or len(block) < 4:
                continue
            rows = parse_nbptx_station(block)
            if "FHR" not in rows:
                continue
            fhr = rows["FHR"]
            if "TXNP1" not in rows or "TXNP5" not in rows or "TXNP9" not in rows:
                continue
            cycle_init = datetime(d.year, d.month, d.day, hh, tzinfo=timezone.utc)
            vts = column_valid_times_utc(cycle_init, fhr)
            j = pick_column_index(vts, fhr, target_date)
            if j is None:
                continue
            p1 = rows["TXNP1"][j] if j < len(rows["TXNP1"]) else None
            p5 = rows["TXNP5"][j] if j < len(rows["TXNP5"]) else None
            p9 = rows["TXNP9"][j] if j < len(rows["TXNP9"]) else None
            if p1 is None or p5 is None or p9 is None:
                continue
            p10, p50, p90 = float(p1), float(p5), float(p9)
            meta = {
                "nbm_source": "nomads_nbptx",
                "nbp_url": url,
                "nbp_cycle_init_utc": cycle_init.isoformat(),
                "nbp_fhr": fhr[j],
                "nbp_valid_utc": vts[j].isoformat() if vts[j] else None,
                "nbp_column_index": j,
            }
            p25v = _nbp_cell_float(rows, "TXNP2", j)
            p75v = _nbp_cell_float(rows, "TXNP7", j)
            sdv = _nbp_cell_float(rows, "TXNSD", j)
            if p25v is not None:
                meta["nbm_p25_raw"] = p25v
            if p75v is not None:
                meta["nbm_p75_raw"] = p75v
            if sdv is not None:
                meta["nbm_sd_raw"] = sdv
            if abs(p50 - nws_p50_f) > 8.0:
                print(
                    f"WARNING: NBP TXNP5 ({p50:.1f}F) vs NWS grid maxT ({nws_p50_f:.1f}F) differ > 8F.",
                    file=sys.stderr,
                )
            return p10, p50, p90, meta
    raise RuntimeError(
        "No NBP bulletin found with a column valid on local date "
        f"{target_date.isoformat()} (America/New_York) with FHR in [6,48]"
    )


def kalshi_integration_bounds(title: str, ticker: str) -> tuple[str, float, float]:
    """
    Convert one Kalshi bracket into numeric integration limits for a continuous CDF.

    Kalshi NHIGH settlement convention (from CFTC filing):
    - "greater than X" = strictly > X (exactly X does NOT pay out)
    - "less than X"    = strictly < X (exactly X does NOT pay out)
    - "between X and Y" = inclusive of both X and Y

    NWS CLI reports integer °F. Outcome space is discrete integers.

    Continuity correction for zone interpolation:
    Since the model uses a continuous probability distribution but outcomes are integers,
    we apply +/- 0.5 to bracket boundaries so that each integer maps to the bracket whose
    continuous interval contains it.
    Example: bracket "62-63" integrates [61.5, 63.5].

    Returns (bracket_label, integration_lower_f, integration_upper_f).
    """
    # Prefer the ticker encoding: ...-B62.5 (between) and ...-T62 / ...-T69 (tails)
    # Direction for tails is read from the title ("<" / ">" or less/greater than).
    m_b = re.search(r"-B(\d+(?:\.\d+)?)\b", ticker)
    if m_b:
        mid = float(m_b.group(1))  # e.g. 62.5 represents 62-63 inclusive
        lo = mid - 1.0
        hi = mid + 1.0
        label = f"{int(mid - 0.5)}-{int(mid + 0.5)}"
        return label, lo, hi

    m_t = re.search(r"-T(\d+(?:\.\d+)?)\b", ticker)
    if m_t:
        x = float(m_t.group(1))
        # Tail direction: title text is unambiguous ("<" / ">" or less/greater than).
        is_hi = bool(re.search(r"greater\s+than|>\s*\d+", title, re.I))
        is_lo = bool(re.search(r"less\s+than|<\s*\d+", title, re.I))
        if is_hi and not is_lo:
            return f">{int(x)}", x + 0.5, float("inf")
        if is_lo and not is_hi:
            return f"<{int(x)}", float("-inf"), x - 0.5
        raise ValueError(f"Cannot infer tail direction from title: {title!r} ({ticker})")

    # Fallback for tests / unexpected tickers: parse the human title and apply correction.
    m_band = re.search(r"be\s+(\d+)\s*-\s*(\d+)\s*°", title, re.I)
    if m_band:
        lo_i = float(m_band.group(1))
        hi_i = float(m_band.group(2))
        return f"{int(lo_i)}-{int(hi_i)}", lo_i - 0.5, hi_i + 0.5
    m_hi = re.search(r">\s*(\d+)\s*°|greater\s+than\s+(\d+)\s*°", title, re.I)
    if m_hi:
        x = float(m_hi.group(1) or m_hi.group(2))
        return f">{int(x)}", x + 0.5, float("inf")
    m_lo = re.search(r"<\s*(\d+)\s*°|less\s+than\s+(\d+)\s*°", title, re.I)
    if m_lo:
        x = float(m_lo.group(1) or m_lo.group(2))
        return f"<{int(x)}", float("-inf"), x - 0.5
    raise ValueError(f"Cannot parse bracket from title/ticker: {title!r} ({ticker})")


def kalshi_settlement_wins(bracket_label: str, actual_max_f: float) -> bool:
    """
    True if the official daily high (°F) settles this NHIGH bracket.

    CFTC NHHIGH convention (must match integration semantics in kalshi_integration_bounds):
    - Tail "greater than X": strictly > X (exactly X does not pay).
    - Tail "less than X": strictly < X.
    - Band "A-B": inclusive of both integer endpoints.

    bracket_label is the short form we store (e.g. "78-79", ">79", "<62").
    """
    s = bracket_label.strip().replace(" ", "")
    m_hi = re.fullmatch(r">(\d+)", s)
    if m_hi:
        return actual_max_f > float(m_hi.group(1))
    m_lo = re.fullmatch(r"<(\d+)", s)
    if m_lo:
        return actual_max_f < float(m_lo.group(1))
    m_mid = re.fullmatch(r"(\d+)-(\d+)", s)
    if m_mid:
        a, b = int(m_mid.group(1)), int(m_mid.group(2))
        lo, hi = (a, b) if a <= b else (b, a)
        return lo <= actual_max_f <= hi
    raise ValueError(f"Unrecognized bracket_label for settlement: {bracket_label!r}")


def kalshi_mid_price(m: dict[str, Any]) -> float:
    """Midpoint of yes bid/ask in [0,1]; fallback last_price_dollars."""
    bid = m.get("yes_bid_dollars")
    ask = m.get("yes_ask_dollars")
    if bid is not None and ask is not None:
        return (float(bid) + float(ask)) / 2.0
    lp = m.get("last_price_dollars")
    if lp is not None:
        return float(lp)
    raise ValueError(f"No price fields for {m.get('ticker')}")


def fetch_kalshi_markets(series_ticker: str, target: date) -> list[dict[str, Any]]:
    """Open markets for series whose ticker encodes target local date (26MAR30)."""
    mon = target.strftime("%b").upper()
    token = target.strftime("%y") + mon + target.strftime("%d")
    qs = urlencode({"series_ticker": series_ticker, "status": "open"})
    url = f"https://api.elections.kalshi.com/trade-api/v2/markets?{qs}"
    data = _http_json(url)
    markets = data.get("markets", [])
    out = [m for m in markets if token in m.get("ticker", "")]
    if not out:
        raise RuntimeError(f"No open Kalshi markets for {series_ticker} on {target} (token {token})")
    return out


def _wind_speed_to_knots_int(val: float, uom: str) -> int:
    """Convert NWS grid wind speed to integer knots (same grid response, no extra fetches)."""
    u = (uom or "").lower()
    # 1 kn = 1.852 km/h (exact definition) — NYC morning winds often ~5–15 kt; sanity-check logs if outside.
    if "km_h" in u or "kmh" in u:
        return int(round(val / 1.852))
    if "m_s" in u or "m s-1" in u:
        return int(round(val * 1.94384))
    if "mph" in u or "mi_h" in u:
        return int(round(val * 0.868976))
    return int(round(val / 1.852))


def _nws_min_temperature_cell_for_local_date(
    props: dict[str, Any], target: date
) -> Optional[tuple[float, str, float]]:
    """
    First grid minTemperature cell whose validity *start* is on target in America/New_York.

    Returns (temp_f, validTime_raw, value_degC) or None. NWS cells are often PT12H/PT24H — the min is
    over that whole window, not strictly “midnight–7am local”; see stderr log in nws_log_context.
    """
    mt = props.get("minTemperature")
    if not mt or "values" not in mt:
        return None
    uom = mt.get("uom", "")
    z = _zone()
    for cell in mt["values"]:
        vt = cell.get("validTime")
        val = cell.get("value")
        if vt is None or val is None:
            continue
        start_utc = parse_iso_duration_start(vt)
        if start_utc.astimezone(z).date() != target:
            continue
        c = float(val)
        if "degC" in uom or "Cel" in uom:
            fahrenheit = c * 9.0 / 5.0 + 32.0
        else:
            fahrenheit = c
        return (fahrenheit, str(vt), c)
    return None


def _fetch_metar_sky_cover_7am(target: date) -> Optional[str]:
    """
    Fetch the most recent METAR skyc1 observation for KNYC in the window
    5:00–8:00am local time on target date. Returns the skyc1 string (e.g.
    'OVC', 'BKN', 'FEW', 'CLR') or None if unavailable.
    """
    z = _zone()
    sts = datetime(target.year, target.month, target.day, 5, 0, tzinfo=z).astimezone(timezone.utc)
    ets = datetime(target.year, target.month, target.day, 8, 0, tzinfo=z).astimezone(timezone.utc)
    params = [
        ("station", "KNYC"),
        ("data", "skyc1"),
        ("tz", "UTC"),
        ("format", "onlycomma"),
        ("sts", sts.strftime("%Y-%m-%dT%H:%M:%SZ")),
        ("ets", ets.strftime("%Y-%m-%dT%H:%M:%SZ")),
    ]
    url = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?" + urlencode(params)
    try:
        req = Request(url, headers={"User-Agent": NWS_USER_AGENT})
        with urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        if raw.lstrip().startswith("["):
            return None
        rows = list(csv.DictReader(io.StringIO(raw)))
        for rec in reversed(rows):
            val = (rec.get("skyc1") or "").strip()
            if val and val != "M":
                return val
    except Exception:
        pass
    return None


def nws_log_context(grid_url: str, target: date) -> dict[str, Any]:
    """
    Wind direction and wind speed at ~7am local from NWS forecast grid; sky cover from METAR skyc1
    (5–8am local KNYC); daily min (°F) from grid minTemperature.

    wind_speed_kt_7am is grid forecast, not METAR — intraday rows use METAR in collector; do not
    treat the two as one series without care (logger/collector comments).
    """
    grid = _http_json(grid_url)
    props = grid["properties"]
    z = _zone()
    t7 = datetime(target.year, target.month, target.day, 7, 0, tzinfo=z).astimezone(timezone.utc)
    ctx: dict[str, Any] = {
        "wind_dir_7am": None,
        "sky_cover_7am": None,
        "wind_speed_kt_7am": None,
        "overnight_low_f": None,
    }
    for key, out_key in (("windDirection", "wind_dir_7am"),):
        series = props.get(key)
        if not series or "values" not in series:
            continue
        for cell in series["values"]:
            vt = cell.get("validTime")
            val = cell.get("value")
            if not vt or val is None:
                continue
            st = parse_iso_duration_start(vt)
            if "/" in vt:
                dur = vt.split("/", 1)[1]
                if dur.startswith("PT") and dur.endswith("H"):
                    hrs = int(dur[2:-1])
                    en = st + timedelta(hours=hrs)
                else:
                    en = st + timedelta(hours=1)
            else:
                en = st + timedelta(hours=1)
            if st <= t7 < en:
                ctx[out_key] = val
                break

    ctx["sky_cover_7am"] = _fetch_metar_sky_cover_7am(target)

    ws_series = props.get("windSpeed")
    if ws_series and "values" in ws_series:
        uom_ws = ws_series.get("uom", "")
        for cell in ws_series["values"]:
            vt = cell.get("validTime")
            val = cell.get("value")
            if not vt or val is None:
                continue
            st = parse_iso_duration_start(vt)
            if "/" in vt:
                dur = vt.split("/", 1)[1]
                if dur.startswith("PT") and dur.endswith("H"):
                    hrs = int(dur[2:-1])
                    en = st + timedelta(hours=hrs)
                else:
                    en = st + timedelta(hours=1)
            else:
                en = st + timedelta(hours=1)
            if st <= t7 < en:
                ctx["wind_speed_kt_7am"] = _wind_speed_to_knots_int(float(val), uom_ws)
                break

    low_cell = _nws_min_temperature_cell_for_local_date(props, target)
    if low_cell is not None:
        low_f, raw_vt, raw_c = low_cell
        ctx["overnight_low_f"] = low_f
        print(
            f"[morning_model] NWS grid minTemperature for local {target}: validTime={raw_vt!r} "
            f"raw_degC={raw_c:.2f} -> overnight_low_f={low_f:.1f}F "
            f"(min is over the cell's full valid window, often 12–24h; confirm vs obs if needed)",
            file=sys.stderr,
        )
    return ctx


def record_proximity_flag(
    target: date, p90_biased_f: float, records_path: Optional[Path]
) -> bool:
    """
    True if p90 is within RECORD_PROXIMITY_F of same-calendar-day record high (PROVISIONAL).

    Expects records.json: {"MMDD": {"record_high_f": float}, ...}. Unknown schema -> False.
    """
    if not records_path or not records_path.is_file():
        return False
    try:
        data = json.loads(records_path.read_text(encoding="utf-8"))
        key = target.strftime("%m%d")
        rec = data.get(key)
        if not rec or "record_high_f" not in rec:
            return False
        rh = float(rec["record_high_f"])
        return abs(p90_biased_f - rh) <= RECORD_PROXIMITY_F
    except (json.JSONDecodeError, TypeError, ValueError, KeyError):
        return False


def run_model(
    target: date,
    nbm_bias: float,
    series_ticker: str,
    pct_f_raw: tuple[float, float, float],
    kalshi_markets: Optional[list[dict[str, Any]]] = None,
    nbm_p25_raw: Optional[float] = None,
    nbm_p75_raw: Optional[float] = None,
) -> list[BracketRow]:
    """Core model: zones from biased NBM triple (optional p25/p75 for 5-knot CDF); optional live Kalshi list."""
    p10, p50, p90 = apply_nbm_bias(pct_f_raw[0], pct_f_raw[1], pct_f_raw[2], nbm_bias)
    p25a = nbm_p25_raw + nbm_bias if nbm_p25_raw is not None else None
    p75a = nbm_p75_raw + nbm_bias if nbm_p75_raw is not None else None
    if p25a is None or p75a is None:
        print(
            f"[morning_model] WARNING: p25/p75 not available for {target}, "
            f"falling back to 3-knot CDF. Check NBP bulletin for TXNP2/TXNP7.",
            file=sys.stderr,
        )
    z = build_zones(p10, p50, p90, p25a, p75a)
    z_triplet = (
        build_zones(p10, p50, p90) if (p25a is not None and p75a is not None) else None
    )
    if kalshi_markets is None:
        kalshi_markets = fetch_kalshi_markets(series_ticker, target)
    rows: list[BracketRow] = []
    for m in kalshi_markets:
        title = str(m.get("title", ""))
        ticker = str(m.get("ticker", ""))
        blabel, lo, hi = kalshi_integration_bounds(title, ticker)
        price = kalshi_mid_price(m)
        bid = m.get("yes_bid_dollars")
        ask = m.get("yes_ask_dollars")
        bid_f = float(bid) if bid is not None else None
        ask_f = float(ask) if ask is not None else None
        mp = bracket_prob(z, lo, hi)
        mp3 = bracket_prob(z_triplet, lo, hi) if z_triplet is not None else None
        rows.append(
            BracketRow(
                ticker=ticker,
                title=title,
                bracket_label=blabel,
                lower_f=lo,
                upper_f=hi,
                market_price=price,
                market_bid=bid_f,
                market_ask=ask_f,
                model_prob=mp,
                edge=mp - price,
                model_prob_triplet_cdf=mp3,
            )
        )
    rows.sort(key=lambda r: r.lower_f if not math.isinf(r.lower_f) else -999)
    # Sanity check: only meaningful when the market list covers both tails (full partition).
    has_low_tail = any(math.isinf(r.lower_f) and r.lower_f < 0 for r in rows)
    has_high_tail = any(math.isinf(r.upper_f) and r.upper_f > 0 for r in rows)
    if has_low_tail and has_high_tail:
        total = sum(r.model_prob for r in rows)
        assert abs(total - 1.0) < 0.001, (
            f"model_prob sum = {total:.6f}, expected 1.0. "
            f"Check bracket boundary continuity correction."
        )
    return rows


def _c_to_f(c: float) -> float:
    return c * 9.0 / 5.0 + 32.0


def _emp_quantile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return float("nan")
    n = len(sorted_vals)
    if n == 1:
        return sorted_vals[0]
    pos = q * (n - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    lo = max(0, min(lo, n - 1))
    hi = max(0, min(hi, n - 1))
    if lo == hi:
        return sorted_vals[lo]
    w = pos - lo
    return sorted_vals[lo] * (1.0 - w) + sorted_vals[hi] * w


def _ensemble_member_stats_f(
    daily: dict[str, Any],
    day_idx: int,
    member_key_re: re.Pattern[str],
) -> Optional[dict[str, float]]:
    vals_c: list[float] = []
    for k, series in daily.items():
        if k == "time" or not isinstance(series, list):
            continue
        if not member_key_re.match(k):
            continue
        if day_idx >= len(series):
            continue
        cell = series[day_idx]
        if cell is None:
            continue
        try:
            vals_c.append(float(cell))
        except (TypeError, ValueError):
            continue
    if len(vals_c) < 2:
        return None
    vals_f = sorted(_c_to_f(c) for c in vals_c)
    return {
        "spread_f": float(max(vals_f) - min(vals_f)),
        "sd_f": float(statistics.pstdev(vals_f)),
        "p10_f": float(_emp_quantile(vals_f, 0.10)),
        "p50_f": float(_emp_quantile(vals_f, 0.50)),
        "p90_f": float(_emp_quantile(vals_f, 0.90)),
    }


def fetch_ensemble_spread(lat: float, lon: float, event_date: date) -> dict[str, Any]:
    """
    Open-Meteo ensemble daily max temperature (logged alongside snapshots; current callers do not fold it into model_prob).

    Pulls GFS seamless (GEFS-like) and ECMWF IFS 0.25° ensemble members; returns a flat dict
    with keys ens_gefs_spread_f, ens_gefs_sd_f, ens_gefs_p50_f, ens_ecmwf_spread_f,
    ens_ecmwf_sd_f, ens_ecmwf_p50_f when available. Requires ensemble-api.open-meteo.com reachability.
    """
    zt = _zone()
    ny_today = datetime.now(zt).date()
    delta = (event_date - ny_today).days
    forecast_days = min(16, max(3, delta + 3))
    base = (
        "https://ensemble-api.open-meteo.com/v1/ensemble"
        f"?latitude={lat}&longitude={lon}&daily=temperature_2m_max"
        f"&forecast_days={forecast_days}&timezone=America%2FNew_York"
    )
    # Per-response keys: with timezone=America/New_York both models use
    # temperature_2m_max_memberNN (no model suffix in the key name).
    re_members = re.compile(r"^temperature_2m_max_member\d+$")
    out: dict[str, Any] = {}
    try:
        j_g = _http_json(base + "&models=gfs_seamless")
        times = (j_g.get("daily") or {}).get("time") or []
        if isinstance(times, list):
            try:
                day_idx = [str(t) for t in times].index(event_date.isoformat())
            except ValueError:
                day_idx = -1
            if day_idx >= 0:
                g = _ensemble_member_stats_f(j_g.get("daily") or {}, day_idx, re_members)
                if g:
                    out["ens_gefs_spread_f"] = g["spread_f"]
                    out["ens_gefs_sd_f"] = g["sd_f"]
                    out["ens_gefs_p50_f"] = g["p50_f"]
    except (HTTPError, URLError, TimeoutError, OSError, TypeError, ValueError, KeyError):
        pass
    try:
        j_e = _http_json(base + "&models=ecmwf_ifs025")
        times = (j_e.get("daily") or {}).get("time") or []
        if isinstance(times, list):
            try:
                day_idx = [str(t) for t in times].index(event_date.isoformat())
            except ValueError:
                day_idx = -1
            if day_idx >= 0:
                e = _ensemble_member_stats_f(j_e.get("daily") or {}, day_idx, re_members)
                if e:
                    out["ens_ecmwf_spread_f"] = e["spread_f"]
                    out["ens_ecmwf_sd_f"] = e["sd_f"]
                    out["ens_ecmwf_p50_f"] = e["p50_f"]
    except (HTTPError, URLError, TimeoutError, OSError, TypeError, ValueError, KeyError):
        pass
    return out


def fetch_hrrr_forecast(
    lat: float,
    lon: float,
    event_date: date,
    as_of_utc: Optional[datetime] = None,
) -> Optional[dict[str, Any]]:
    """
    Open-Meteo HRRR CONUS hourly temperature forecast.

    Used by collector intraday paths; morning_model may call this if wired in.

    Returns:
      {"hrrr_max_f": float, "hrrr_current_hour_f": float, "hrrr_model_run": str}
    or None on failure.
    """
    try:
        if as_of_utc is None:
            as_of_utc = datetime.now(timezone.utc)
        if as_of_utc.tzinfo is None:
            as_of_utc = as_of_utc.replace(tzinfo=timezone.utc)
        z = _zone()
        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            "&hourly=temperature_2m"
            "&models=gfs_hrrr"
            "&timezone=America%2FNew_York"
            "&forecast_days=2"
        )
        try:
            j = _http_json(url)
        except Exception as e:
            if "503" in str(e) or "Service Unavailable" in str(e):
                import time
                time.sleep(3)
                j = _http_json(url)
            else:
                raise
        hourly = j.get("hourly") or {}
        times = hourly.get("time") or []
        temps = hourly.get("temperature_2m") or []
        if not isinstance(times, list) or not isinstance(temps, list) or len(times) != len(temps):
            return None
        units = (j.get("hourly_units") or {}).get("temperature_2m") or ""
        is_c = "°C" in str(units) or str(units).strip() == "C"

        vals_f: list[float] = []
        best_now: Optional[Tuple[datetime, float]] = None
        as_of_local = as_of_utc.astimezone(z)
        for t_raw, v_raw in zip(times, temps):
            if v_raw is None:
                continue
            try:
                v = float(v_raw)
            except (TypeError, ValueError):
                continue
            try:
                dt_local = datetime.fromisoformat(str(t_raw))
            except ValueError:
                continue
            if dt_local.tzinfo is None:
                dt_local = dt_local.replace(tzinfo=z)
            else:
                dt_local = dt_local.astimezone(z)

            v_f = _c_to_f(v) if is_c else v
            if dt_local.date() == event_date:
                vals_f.append(v_f)
            if dt_local <= as_of_local:
                if best_now is None or dt_local > best_now[0]:
                    best_now = (dt_local, v_f)
        if not vals_f or best_now is None:
            return None
        return {
            "hrrr_max_f": float(max(vals_f)),
            "hrrr_current_hour_f": float(best_now[1]),
            "hrrr_model_run": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
    except Exception:
        return None


def truncate_and_renormalize(rows: list[BracketRow], observed_max_f: float) -> list[BracketRow]:
    """
    Hard-floor by observed running max: brackets entirely below observed_max are dead.

    Continuity convention: a bracket with upper_f <= observed_max + 0.5 is impossible
    once METAR has observed observed_max as the running high.
    """
    cutoff = float(observed_max_f) + 0.5
    staged: list[BracketRow] = []
    total = 0.0
    for r in rows:
        p = float(r.model_prob)
        if r.upper_f <= cutoff:
            p = 0.0
        total += p
        staged.append(replace(r, model_prob=p))
    if total <= 0.0:
        import sys as _sys
        print("truncate_and_renormalize: all brackets zeroed after truncation — returning original rows untruncated", file=_sys.stderr)
        return rows
    out: list[BracketRow] = []
    for r in staged:
        p = r.model_prob / total if r.model_prob > 0 else 0.0
        out.append(replace(r, model_prob=p, edge=p - r.market_price))
    return out


def _shift_zone(z: ZoneModel, shift_f: float) -> ZoneModel:
    return ZoneModel(
        L=z.L + shift_f,
        p10=z.p10 + shift_f,
        p50=z.p50 + shift_f,
        p90=z.p90 + shift_f,
        U=z.U + shift_f,
        p25=(z.p25 + shift_f) if z.p25 is not None else None,
        p75=(z.p75 + shift_f) if z.p75 is not None else None,
    )


def apply_hrrr_shift(
    z: ZoneModel,
    hrrr_max_f: float,
    nbm_p50_adj: float,
    rows: list[BracketRow],
    shift_threshold_f: float = 1.0,  # PROVISIONAL
    blend_weight: float = 0.5,  # PROVISIONAL
) -> list[BracketRow]:
    """
    Soft shift of the CDF center based on HRRR-implied daily max.

    shift_f = hrrr_max_f - nbm_p50_adj. If |shift_f| < threshold: no change.
    Otherwise, shift all CDF knots by shift_f * blend_weight and recompute bracket probs.
    """
    shift_f = float(hrrr_max_f) - float(nbm_p50_adj)
    if abs(shift_f) < float(shift_threshold_f):
        return rows
    applied = shift_f * float(blend_weight)
    z2 = _shift_zone(z, applied)
    out: list[BracketRow] = []
    for r in rows:
        mp = bracket_prob(z2, r.lower_f, r.upper_f)
        out.append(replace(r, model_prob=mp, edge=mp - r.market_price))
    # Renormalize if full partition present.
    has_low_tail = any(math.isinf(r.lower_f) and r.lower_f < 0 for r in out)
    has_high_tail = any(math.isinf(r.upper_f) and r.upper_f > 0 for r in out)
    if has_low_tail and has_high_tail:
        total = sum(r.model_prob for r in out)
        if total > 0:
            out = [
                replace(r, model_prob=(r.model_prob / total), edge=(r.model_prob / total) - r.market_price)
                for r in out
            ]
    return out


def fetch_sunrise_sunset(
    lat: float,
    lon: float,
    event_date: date,
) -> tuple[datetime, datetime]:
    """
    Return (sunrise_utc, sunset_utc) for event_date at (lat, lon).

    Hits Open-Meteo forecast endpoint with daily=sunrise,sunset.
    Fallback to 06:15 and 19:45 local (America/New_York) if the API fails.
    Fallback times are PROVISIONAL estimates for NYC spring; actual sunrise
    ranges ~05:30–07:00 ET across the year.
    """
    _FALLBACK_SUNRISE_H, _FALLBACK_SUNRISE_M = 6, 15   # PROVISIONAL
    _FALLBACK_SUNSET_H, _FALLBACK_SUNSET_M = 19, 45    # PROVISIONAL
    z = _zone()
    try:
        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            "&daily=sunrise,sunset"
            "&timezone=America%2FNew_York"
            f"&start_date={event_date.isoformat()}"
            f"&end_date={event_date.isoformat()}"
        )
        j = _http_json(url)
        daily = j.get("daily") or {}
        times = [str(t) for t in (daily.get("time") or [])]
        sunrises = daily.get("sunrise") or []
        sunsets = daily.get("sunset") or []
        if event_date.isoformat() in times:
            idx = times.index(event_date.isoformat())
            if idx < len(sunrises) and idx < len(sunsets) and sunrises[idx] and sunsets[idx]:
                sr_raw = str(sunrises[idx])
                ss_raw = str(sunsets[idx])
                sr_local = datetime.fromisoformat(sr_raw)
                ss_local = datetime.fromisoformat(ss_raw)
                if sr_local.tzinfo is None:
                    sr_local = sr_local.replace(tzinfo=z)
                else:
                    sr_local = sr_local.astimezone(z)
                if ss_local.tzinfo is None:
                    ss_local = ss_local.replace(tzinfo=z)
                else:
                    ss_local = ss_local.astimezone(z)
                return sr_local.astimezone(timezone.utc), ss_local.astimezone(timezone.utc)
    except Exception:
        pass
    sr_fb = datetime(
        event_date.year, event_date.month, event_date.day,
        _FALLBACK_SUNRISE_H, _FALLBACK_SUNRISE_M, tzinfo=z,
    ).astimezone(timezone.utc)
    ss_fb = datetime(
        event_date.year, event_date.month, event_date.day,
        _FALLBACK_SUNSET_H, _FALLBACK_SUNSET_M, tzinfo=z,
    ).astimezone(timezone.utc)
    return sr_fb, ss_fb


def apply_ensemble_width(
    z: ZoneModel,
    ens_gefs_sd_f: Optional[float],
    ens_ecmwf_sd_f: Optional[float],
    nbm_spread_raw: float,
) -> tuple[ZoneModel, Optional[float]]:
    """
    Scale tail extent (L and U only) based on ensemble SD vs NBM-implied SD.

    combined_ens_sd = member-count-weighted mean of available ensemble SDs
        (GEFS 31 members, ECMWF 51 members — PROVISIONAL counts).
    nbm_implied_sd = nbm_spread_raw / 2.56  (p10–p90 ≈ 2.56σ for Gaussian).
    ratio = combined_ens_sd / nbm_implied_sd.

    ratio > 1.15 → widen tails (L moves left, U moves right).
    ratio < 0.85 → narrow tails (L moves right, U moves left).
    Otherwise   → no change.

    Inner knots (p10, p25, p50, p75, p90) are never touched — ensemble spread
    affects tail uncertainty, not the central forecast.  Both thresholds PROVISIONAL.

    Returns (adjusted_zone, ratio). ratio is None when inputs are insufficient.
    """
    _WIDEN_THRESHOLD = 1.15   # PROVISIONAL
    _NARROW_THRESHOLD = 0.85  # PROVISIONAL
    _GEFS_MEMBERS = 31        # PROVISIONAL member count
    _ECMWF_MEMBERS = 51       # PROVISIONAL member count

    if ens_gefs_sd_f is None and ens_ecmwf_sd_f is None:
        return z, None
    if nbm_spread_raw is None or float(nbm_spread_raw) <= 0:
        return z, None

    w_sum = 0.0
    w_tot = 0.0
    if ens_gefs_sd_f is not None and ens_gefs_sd_f > 0:
        w_sum += ens_gefs_sd_f * _GEFS_MEMBERS
        w_tot += _GEFS_MEMBERS
    if ens_ecmwf_sd_f is not None and ens_ecmwf_sd_f > 0:
        w_sum += ens_ecmwf_sd_f * _ECMWF_MEMBERS
        w_tot += _ECMWF_MEMBERS
    if w_tot == 0.0:
        return z, None

    combined_ens_sd = w_sum / w_tot
    nbm_implied_sd = float(nbm_spread_raw) / 2.56  # p10-p90 ≈ 2.56σ for Gaussian
    if nbm_implied_sd <= 0:
        return z, None

    ratio = combined_ens_sd / nbm_implied_sd

    if ratio > _WIDEN_THRESHOLD or ratio < _NARROW_THRESHOLD:
        new_L = z.p10 - (z.p10 - z.L) * ratio
        new_U = z.p90 + (z.U - z.p90) * ratio
        adjusted = ZoneModel(
            L=new_L,
            p10=z.p10,
            p25=z.p25,
            p50=z.p50,
            p75=z.p75,
            p90=z.p90,
            U=new_U,
        )
        return adjusted, ratio

    return z, ratio


def hrrr_blend_weight_for_lead(forecast_lead_hours: float) -> float:
    """
    Lead-time-dependent HRRR blend weight for use with apply_hrrr_shift().

    HRRR's advantage over NBM comes from hourly data assimilation; that edge
    fades as lead time lengthens and NBM's multi-model blend takes over.

    Schedule (all values PROVISIONAL — calibrate against observed |error| data):
      > 12 h : 0.0  — NBM dominates; do not shift
       6–12 h : 0.3  — mild HRRR influence
        3–6 h : 0.5  — equal blend
        < 3 h : 0.7  — HRRR leads
    """
    if forecast_lead_hours <= 0.0:   # past assumed peak — truncation handles it
        return 0.0
    if forecast_lead_hours > 12.0:   # PROVISIONAL
        return 0.0
    if forecast_lead_hours > 6.0:    # PROVISIONAL
        return 0.3                   # PROVISIONAL
    if forecast_lead_hours > 3.0:    # PROVISIONAL
        return 0.5                   # PROVISIONAL
    return 0.7                       # PROVISIONAL


def compute_trajectory_deviation(
    db_path: Path,
    event_date: date,
    forecast_high_f: float,
    overnight_low_f: float,
    sunrise_utc: datetime,
    as_of_utc: datetime,
    station: str = "KNYC",
) -> tuple[Optional[float], Optional[float]]:
    """
    Compare observed METAR temperatures against the expected sinusoidal warming curve.

    Expected diurnal model:
        T_expected(t) = overnight_low + (forecast_high - overnight_low) × sin(π/2 × elapsed/peak_span)
    where elapsed = seconds since sunrise, peak_span = seconds from sunrise to 14:00 local.
    Peak hour 14:00 local is PROVISIONAL — KNYC peak is typically 14:00–15:00 ET.

    Queries metar_observations for KNYC observations in [sunrise_utc, as_of_utc].
    Requires ≥ 3 valid observations (PROVISIONAL minimum); returns (None, None) otherwise.

    Confidence factor scales with as_of_utc local hour (all thresholds PROVISIONAL):
        < 10:00  → 0.3  (morning noisy, sea breeze not formed)
        10–12:00 → 0.5  (pattern establishing)
        12–14:00 → 0.7  (peak approaching)
        ≥ 14:00  → 0.9  (high nearly determined)

    Returns (deviation_f, confidence_factor) or (None, None).
    """
    _PEAK_HOUR_LOCAL = 14   # PROVISIONAL — diurnal max near 2pm local
    _MIN_OBS = 3            # PROVISIONAL — minimum observations required

    if as_of_utc.tzinfo is None:
        as_of_utc = as_of_utc.replace(tzinfo=timezone.utc)
    if sunrise_utc.tzinfo is None:
        sunrise_utc = sunrise_utc.replace(tzinfo=timezone.utc)

    z = _zone()
    sunrise_local = sunrise_utc.astimezone(z)
    peak_local = sunrise_local.replace(hour=_PEAK_HOUR_LOCAL, minute=0, second=0, microsecond=0)
    if peak_local <= sunrise_local:
        peak_local = peak_local + timedelta(days=1)
    peak_utc = peak_local.astimezone(timezone.utc)
    peak_span_secs = (peak_utc - sunrise_utc).total_seconds()
    if peak_span_secs <= 0:
        return None, None

    as_of_s = as_of_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    sunrise_s = sunrise_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        conn = sqlite3.connect(str(db_path))
        try:
            obs_rows = conn.execute(
                """
                SELECT observation_ts, tmpf FROM metar_observations
                WHERE station = ?
                  AND tmpf IS NOT NULL
                  AND observation_ts >= ?
                  AND observation_ts <= ?
                ORDER BY observation_ts
                """,
                (station, sunrise_s, as_of_s),
            ).fetchall()
        finally:
            conn.close()
    except Exception:
        return None, None

    if len(obs_rows) < _MIN_OBS:
        return None, None

    deviations: list[float] = []
    for obs_ts_s, tmpf in obs_rows:
        try:
            obs_dt = datetime.fromisoformat(str(obs_ts_s).replace("Z", "+00:00"))
            if obs_dt.tzinfo is None:
                obs_dt = obs_dt.replace(tzinfo=timezone.utc)
            else:
                obs_dt = obs_dt.astimezone(timezone.utc)
            elapsed = (obs_dt - sunrise_utc).total_seconds()
            if elapsed < 0:
                continue
            t = min(elapsed / peak_span_secs, 1.0)
            sin_factor = math.sin(math.pi / 2.0 * t)
            t_expected = overnight_low_f + (forecast_high_f - overnight_low_f) * sin_factor
            deviations.append(float(tmpf) - t_expected)
        except Exception:
            continue

    if len(deviations) < _MIN_OBS:
        return None, None

    deviation_f = sum(deviations) / len(deviations)

    as_of_local = as_of_utc.astimezone(z)
    hour_local = as_of_local.hour + as_of_local.minute / 60.0
    if hour_local < 10.0:    # PROVISIONAL
        confidence_factor = 0.3  # PROVISIONAL — morning data noisy
    elif hour_local < 12.0:  # PROVISIONAL
        confidence_factor = 0.5  # PROVISIONAL — pattern establishing
    elif hour_local < 14.0:  # PROVISIONAL
        confidence_factor = 0.7  # PROVISIONAL — peak approaching
    else:
        confidence_factor = 0.9  # PROVISIONAL — high nearly determined

    return deviation_f, confidence_factor


def combine_shifts(
    hrrr_shift_f: Optional[float],
    trajectory_shift_f: Optional[float],
) -> Optional[float]:
    """
    Combine HRRR and trajectory deviation shifts into a single CDF shift.

    Agreement (same sign) → amplify: max(|a|, |b|) × 1.2, preserving sign.
    Disagreement (opposite sign) → mute: arithmetic average.
    One signal absent (None) → use the other unchanged.
    Both absent → None.

    Boost factor 1.2 is PROVISIONAL.
    """
    _BOOST = 1.2  # PROVISIONAL — same-sign agreement amplifies the shift

    if hrrr_shift_f is None and trajectory_shift_f is None:
        return None
    if hrrr_shift_f is None:
        return trajectory_shift_f
    if trajectory_shift_f is None:
        return hrrr_shift_f

    if (hrrr_shift_f >= 0.0) == (trajectory_shift_f >= 0.0):
        sign = 1.0 if hrrr_shift_f >= 0.0 else -1.0
        return sign * max(abs(hrrr_shift_f), abs(trajectory_shift_f)) * _BOOST
    else:
        return (hrrr_shift_f + trajectory_shift_f) / 2.0


def apply_trajectory_shift(
    z: ZoneModel,
    trajectory_deviation_f: Optional[float],
    confidence_factor: Optional[float],
    rows: list[BracketRow],
) -> list[BracketRow]:
    """
    Soft shift of the CDF based on METAR trajectory deviation.

    shift_f = trajectory_deviation_f × confidence_factor.
    If either input is None, rows are returned unchanged.
    Uses _shift_zone() — same pattern as apply_hrrr_shift().
    """
    if trajectory_deviation_f is None or confidence_factor is None:
        return rows
    shift_f = float(trajectory_deviation_f) * float(confidence_factor)
    if shift_f == 0.0:
        return rows
    z2 = _shift_zone(z, shift_f)
    out: list[BracketRow] = []
    for r in rows:
        mp = bracket_prob(z2, r.lower_f, r.upper_f)
        out.append(replace(r, model_prob=mp, edge=mp - r.market_price))
    has_low_tail = any(math.isinf(r.lower_f) and r.lower_f < 0 for r in out)
    has_high_tail = any(math.isinf(r.upper_f) and r.upper_f > 0 for r in out)
    if has_low_tail and has_high_tail:
        total = sum(r.model_prob for r in out)
        if total > 0:
            out = [
                replace(r, model_prob=(r.model_prob / total), edge=(r.model_prob / total) - r.market_price)
                for r in out
            ]
    return out


def fetch_live_nbm_fahrenheit(
    lat: float,
    lon: float,
    target: date,
    station: str = NBP_STATION,
) -> tuple[tuple[float, float, float], dict[str, Any]]:
    """NBP text TXNP1/5/9 in °F plus metadata.  station defaults to KNYC."""
    grid_url = nws_grid_url_for_point(lat, lon)
    valid_start, nws_p50_f = nws_daily_max_period_for_date(grid_url, target)
    p10, p50, p90, meta = fetch_pctmax_from_nbp_text(station, valid_start, nws_p50_f)
    return (p10, p50, p90), meta


def synthetic_self_test() -> None:
    """Hardcoded checks before any live pull (project_docs gate)."""
    bias = 0.0
    z = build_zones(50.0, 60.0, 70.0)
    assert abs(zone_cdf(z, 60.0) - 0.5) < 1e-9
    z5 = build_zones(48.0, 60.0, 72.0, 54.0, 66.0)
    assert abs(zone_cdf(z5, 60.0) - 0.5) < 1e-6
    # Truncation renormalizes and only kills fully-below brackets
    rows2 = [
        BracketRow("t1", "x", "<62", float("-inf"), 61.5, 0.1, 0.1, 0.0),
        BracketRow("b", "x", "62-63", 61.5, 63.5, 0.1, 0.2, 0.1),
        BracketRow("t2", "x", ">69", 69.5, float("inf"), 0.1, 0.7, 0.6),
    ]
    tr = truncate_and_renormalize(rows2, observed_max_f=62.0)
    assert tr[0].model_prob == 0.0
    assert abs(sum(r.model_prob for r in tr) - 1.0) < 1e-9
    p_mid = bracket_prob(z, 59.0, 61.0)
    assert 0.05 < p_mid < 0.25, p_mid
    p_tail = bracket_prob(z, 75.0, float("inf"))
    assert 0.0 < p_tail < 0.2, p_tail
    rows = run_model(
        date(2026, 3, 30),
        bias,
        "KXHIGHNY",
        (50.0, 60.0, 70.0),
        kalshi_markets=[
            {
                "ticker": "KXHIGHNY-26MAR30-B59.5",
                "title": "Will the **high temp in NYC** be 59-60° on Mar 30, 2026?",
                "yes_bid_dollars": "0.40",
                "yes_ask_dollars": "0.42",
            }
        ],
    )
    assert len(rows) == 1 and rows[0].ticker.endswith("B59.5")
    assert rows[0].edge == rows[0].model_prob - rows[0].market_price
    assert kalshi_settlement_wins("78-79", 79.0)
    assert not kalshi_settlement_wins(">79", 79.0)
    assert kalshi_settlement_wins(">79", 80.0)
    assert kalshi_settlement_wins("<62", 61.0)
    assert not kalshi_settlement_wins("<62", 62.0)

    # --- hrrr_blend_weight_for_lead ---
    assert hrrr_blend_weight_for_lead(20.0) == 0.0   # > 12 → no shift
    assert hrrr_blend_weight_for_lead(12.1) == 0.0   # just above 12
    assert hrrr_blend_weight_for_lead(12.0) == 0.3   # 12 is not > 12, falls to 6–12 band
    assert hrrr_blend_weight_for_lead(9.0) == 0.3    # 6–12 h band
    assert hrrr_blend_weight_for_lead(6.0) == 0.5    # 6 is not > 6, falls to 3–6 band
    assert hrrr_blend_weight_for_lead(4.5) == 0.5    # 3–6 h band
    assert hrrr_blend_weight_for_lead(3.0) == 0.7    # 3 is not > 3, falls to < 3 band
    assert hrrr_blend_weight_for_lead(1.0) == 0.7    # < 3 h band

    # --- apply_ensemble_width ---
    _z_ew = build_zones(50.0, 60.0, 70.0)
    # p10=50, p90=70 → nbm_spread=20 → nbm_implied_sd ≈ 7.81
    _nbm_sp = 20.0
    # neutral: both SDs ≈ implied_sd → ratio ≈ 1.0, no tail change
    _z_neut, _r_neut = apply_ensemble_width(_z_ew, 7.81, 7.81, _nbm_sp)
    assert _r_neut is not None and 0.85 <= _r_neut <= 1.15, _r_neut
    assert _z_neut.L == _z_ew.L and _z_neut.U == _z_ew.U  # unchanged
    assert _z_neut.p50 == _z_ew.p50
    # wide: both SDs >> implied → ratio > 1.15 → tails widen
    _z_wide, _r_wide = apply_ensemble_width(_z_ew, 12.0, 12.0, _nbm_sp)
    assert _r_wide is not None and _r_wide > 1.15, _r_wide
    assert _z_wide.L < _z_ew.L and _z_wide.U > _z_ew.U  # tails widened
    assert _z_wide.p10 == _z_ew.p10 and _z_wide.p50 == _z_ew.p50 and _z_wide.p90 == _z_ew.p90
    # narrow: both SDs << implied → ratio < 0.85 → tails narrow
    _z_narr, _r_narr = apply_ensemble_width(_z_ew, 3.0, 3.0, _nbm_sp)
    assert _r_narr is not None and _r_narr < 0.85, _r_narr
    assert _z_narr.L > _z_ew.L and _z_narr.U < _z_ew.U  # tails narrowed
    assert _z_narr.p50 == _z_ew.p50
    # both None → unchanged, ratio None
    _z_n2, _r_n2 = apply_ensemble_width(_z_ew, None, None, _nbm_sp)
    assert _r_n2 is None and _z_n2.L == _z_ew.L
    # one side None: only ECMWF available
    _z_ecmw, _r_ecmw = apply_ensemble_width(_z_ew, None, 12.0, _nbm_sp)
    assert _r_ecmw is not None and _r_ecmw > 1.15
    assert _z_ecmw.L < _z_ew.L

    # --- combine_shifts ---
    # same positive sign → max × boost
    _cs1 = combine_shifts(2.0, 1.5)
    assert abs(_cs1 - 2.4) < 1e-9, _cs1   # max(2.0, 1.5)=2.0 × 1.2=2.4
    # same negative sign → max magnitude × boost, negative
    _cs2 = combine_shifts(-2.0, -1.5)
    assert abs(_cs2 - (-2.4)) < 1e-9, _cs2
    # opposite sign → average (muted)
    _cs3 = combine_shifts(2.0, -1.0)
    assert abs(_cs3 - 0.5) < 1e-9, _cs3   # (2.0 + (-1.0)) / 2 = 0.5
    # opposite sign, negative result
    _cs4 = combine_shifts(-2.0, 1.0)
    assert abs(_cs4 - (-0.5)) < 1e-9, _cs4
    # one None → pass through the other
    assert combine_shifts(None, 2.0) == 2.0
    assert combine_shifts(2.0, None) == 2.0
    assert combine_shifts(None, None) is None
    # zero shift stays zero
    _cs5 = combine_shifts(0.0, 0.0)
    assert _cs5 == 0.0

    # --- apply_trajectory_shift ---
    # Use bracket that straddles p90=70 so the shift crosses the slope-change knot.
    # Symmetric zones have equal slopes in [p10,p50] and [p50,p90]; probability within
    # either segment is shift-invariant.  Brackets crossing p10 or p90 are not.
    _z_traj = build_zones(50.0, 60.0, 70.0)
    _rows_traj = run_model(
        date(2026, 4, 10),
        0.0,
        "KXHIGHNY",
        (50.0, 60.0, 70.0),
        kalshi_markets=[
            {
                "ticker": "KXHIGHNY-26APR10-B70.5",
                "title": "Will the high temp in NYC be 70-71° on Apr 10?",
                "yes_bid_dollars": "0.03",
                "yes_ask_dollars": "0.04",
            }
        ],
    )
    _mp_orig = _rows_traj[0].model_prob
    # shift 2.0°F × 0.5 confidence = 1.0°F; bracket [69.5, 71.5] straddles p90=70
    # — inner vs tail slope differ, so probability changes
    _rows_shifted = apply_trajectory_shift(_z_traj, 2.0, 0.5, _rows_traj)
    assert _rows_shifted[0].model_prob != _mp_orig, (
        f"Trajectory shift did not change model_prob (bracket near p90 expected to change; "
        f"orig={_mp_orig:.4f} new={_rows_shifted[0].model_prob:.4f})"
    )
    # None deviation → no change
    _rows_none = apply_trajectory_shift(_z_traj, None, 0.5, _rows_traj)
    assert _rows_none[0].model_prob == _mp_orig
    # None confidence → no change
    _rows_none2 = apply_trajectory_shift(_z_traj, 2.0, None, _rows_traj)
    assert _rows_none2[0].model_prob == _mp_orig

    # --- compute_trajectory_deviation ---
    _sr_utc = datetime(2026, 4, 10, 10, 20, tzinfo=timezone.utc)  # ~6:20am ET
    _as_of = _sr_utc + timedelta(hours=3)   # ~9:20am ET → < 10am → confidence 0.3
    _fd, _tmp = tempfile.mkstemp(suffix=".db")
    os.close(_fd)
    try:
        _conn = sqlite3.connect(_tmp)
        _conn.execute(
            """CREATE TABLE metar_observations (
                observation_ts TEXT NOT NULL, station TEXT,
                tmpf REAL, wind_dir_deg INTEGER,
                wind_speed_kt INTEGER, sky_cover TEXT, fetch_ts TEXT
            )"""
        )
        # Insert 4 observations starting at sunrise, warming 2°F/hr
        for _i in range(4):
            _ts = (_sr_utc + timedelta(hours=_i)).strftime("%Y-%m-%dT%H:%M:%SZ")
            _conn.execute(
                "INSERT INTO metar_observations (observation_ts, station, tmpf) VALUES (?, 'KNYC', ?)",
                (_ts, 50.0 + _i * 2.0),
            )
        _conn.commit()
        _conn.close()
        _dev, _conf = compute_trajectory_deviation(
            Path(_tmp),
            date(2026, 4, 10),
            forecast_high_f=70.0,
            overnight_low_f=45.0,
            sunrise_utc=_sr_utc,
            as_of_utc=_as_of,
        )
        assert _dev is not None, f"Expected deviation, got None"
        assert _conf is not None, f"Expected confidence, got None"
        assert 0.0 < _conf <= 1.0, _conf
        # with as_of_utc at 9:20am ET (hour_local < 10) → confidence should be 0.3  # PROVISIONAL
        assert abs(_conf - 0.3) < 1e-9, f"Expected 0.3 confidence before 10am, got {_conf}"
        # Fewer than 3 obs → (None, None)
        _conn2 = sqlite3.connect(_tmp)
        _conn2.execute("DELETE FROM metar_observations")
        _conn2.execute(
            "INSERT INTO metar_observations (observation_ts, station, tmpf) VALUES (?, 'KNYC', ?)",
            (_sr_utc.strftime("%Y-%m-%dT%H:%M:%SZ"), 52.0),
        )
        _conn2.execute(
            "INSERT INTO metar_observations (observation_ts, station, tmpf) VALUES (?, 'KNYC', ?)",
            ((_sr_utc + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"), 53.0),
        )
        _conn2.commit()
        _conn2.close()
        _dev2, _conf2 = compute_trajectory_deviation(
            Path(_tmp), date(2026, 4, 10), 70.0, 45.0, _sr_utc, _as_of
        )
        assert _dev2 is None and _conf2 is None, f"Expected (None, None) with 2 obs, got ({_dev2}, {_conf2})"
    finally:
        os.unlink(_tmp)

    # --- fetch_sunrise_sunset (type/order check; API or fallback both valid) ---
    _sr_dt, _ss_dt = fetch_sunrise_sunset(KNYC_LAT, KNYC_LON, date(2026, 4, 10))
    assert isinstance(_sr_dt, datetime) and _sr_dt.tzinfo is not None, "sunrise must be UTC-aware datetime"
    assert isinstance(_ss_dt, datetime) and _ss_dt.tzinfo is not None, "sunset must be UTC-aware datetime"
    assert _sr_dt < _ss_dt, f"sunrise {_sr_dt} must be before sunset {_ss_dt}"

    print("synthetic_self_test: ok")


def main() -> int:
    ap = argparse.ArgumentParser(description="v0 morning_model (KNYC / Kalshi KXHIGHNY)")
    ap.add_argument("--date", type=str, default="", help="Target local date YYYY-MM-DD (default: today NY).")
    ap.add_argument("--config", type=Path, default=Path(__file__).resolve().parent / "config.json")
    ap.add_argument("--series", type=str, default=DEFAULT_KALSHI_SERIES)
    ap.add_argument("--mock-nbm", type=str, default="", help="Comma p10,p50,p90 in °F (skip NBP download).")
    ap.add_argument("--no-log", action="store_true", help="Skip SQLite log and stderr bracket table.")
    ap.add_argument("--log-db", type=Path, default=None, help="SQLite file (default: nyc_temp_log.sqlite beside config).")
    ap.add_argument("--test", action="store_true", help="Run built-in synthetic checks and exit.")
    args = ap.parse_args()
    if args.test:
        synthetic_self_test()
        return 0

    zt = _zone()
    if args.date:
        target = date.fromisoformat(args.date)
    else:
        target = datetime.now(zt).date()

    nbm_bias = load_config(args.config)
    nbp_meta: dict[str, Any] = {}
    if args.mock_nbm:
        parts = [float(x.strip()) for x in args.mock_nbm.split(",")]
        if len(parts) != 3:
            raise SystemExit("--mock-nbm expects p10,p50,p90")
        pct_f = (parts[0], parts[1], parts[2])
    else:
        pct_f, nbp_meta = fetch_live_nbm_fahrenheit(KNYC_LAT, KNYC_LON, target)

    p25r = nbp_meta.get("nbm_p25_raw") if nbp_meta else None
    p75r = nbp_meta.get("nbm_p75_raw") if nbp_meta else None
    try:
        p25f = float(p25r) if p25r is not None else None
    except (TypeError, ValueError):
        p25f = None
    try:
        p75f = float(p75r) if p75r is not None else None
    except (TypeError, ValueError):
        p75f = None
    rows = run_model(
        target,
        nbm_bias,
        args.series,
        pct_f,
        nbm_p25_raw=p25f,
        nbm_p75_raw=p75f,
    )
    pull_ts = datetime.now(timezone.utc)
    p10b, p50b, p90b = apply_nbm_bias(pct_f[0], pct_f[1], pct_f[2], nbm_bias)
    out: dict[str, Any] = {
        "target_date_local": target.isoformat(),
        "nbm_bias_f": nbm_bias,
        "nbm_pctmax_f_raw": {"p10": pct_f[0], "p50": pct_f[1], "p90": pct_f[2]},
        "nbm_pctmax_f_biased": {"p10": p10b, "p50": p50b, "p90": p90b},
        "record_proximity_flag": record_proximity_flag(
            target, p90b, Path(__file__).resolve().parent / "records.json"
        ),
        "brackets": [
            {
                "ticker": r.ticker,
                "lower_f": r.lower_f,
                "upper_f": r.upper_f,
                "market_price": round(r.market_price, 4),
                "model_prob": round(r.model_prob, 4),
                "edge": round(r.edge, 4),
            }
            for r in rows
        ],
    }
    if nbp_meta:
        out["nbm_fetch"] = nbp_meta
    try:
        gu = nws_grid_url_for_point(KNYC_LAT, KNYC_LON)
        out["nws_log_context"] = nws_log_context(gu, target)
    except Exception as e:
        out["nws_log_context"] = {"error": str(e)}

    def _json_safe(x: Any) -> Any:
        if isinstance(x, dict):
            return {k: _json_safe(v) for k, v in x.items()}
        if isinstance(x, list):
            return [_json_safe(v) for v in x]
        if isinstance(x, float) and math.isinf(x):
            return "inf" if x > 0 else "-inf"
        return x

    print(json.dumps(_json_safe(out), indent=2))
    if not args.no_log:
        from logger import DEFAULT_DB_NAME, log_morning_run, print_terminal_review

        log_db = args.log_db if args.log_db is not None else Path(__file__).resolve().parent / DEFAULT_DB_NAME
        ens: dict[str, Any] = {}
        try:
            ens = fetch_ensemble_spread(KNYC_LAT, KNYC_LON, target)
        except Exception:
            pass
        log_morning_run(
            log_db,
            target,
            rows,
            pct_f,
            nbm_bias,
            bool(out["record_proximity_flag"]),
            out.get("nws_log_context") or {},
            pull_ts,
            "morning",
            out.get("nbm_fetch") or {},
            Path(__file__).resolve().parent / "records.json",
            ensemble_snap=ens,
        )
        print_terminal_review(target, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
