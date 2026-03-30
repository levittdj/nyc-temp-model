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
import json
import math
import re
import sys
from dataclasses import dataclass
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


def load_config(path: Path) -> float:
    """Load nbm_bias (°F) from config.json."""
    data = json.loads(path.read_text(encoding="utf-8"))
    if "nbm_bias" not in data:
        raise KeyError("config.json must contain nbm_bias")
    return float(data["nbm_bias"])


def build_zones(p10: float, p50: float, p90: float) -> ZoneModel:
    """
    Build asymmetric zone CDF support from biased percentiles.

    Inner knots: F(p10)=0.1, F(p50)=0.5, F(p90)=0.9 with linear segments.
    Lower tail: F(L)=0 with L = 2*p10 - p50; upper tail: F(U)=1 with U = 2*p90 - p50.
    """
    L = 2.0 * p10 - p50
    U = 2.0 * p90 - p50
    if not (L < p10 < p50 < p90 < U):
        raise ValueError(f"Invalid zone order after bias: L={L} p10={p10} p50={p50} p90={p90} U={U}")
    return ZoneModel(L=L, p10=p10, p50=p50, p90=p90, U=U)


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


def pick_column_index(
    valid_times: list[Optional[datetime]],
    fhr_row: list[Optional[int]],
    target_date: date,
) -> Optional[int]:
    """
    Column whose valid UTC falls on target_date in America/New_York; FHR in [6, 48].
    If multiple qualify, lowest FHR (shortest lead time) wins.
    """
    z = _zone()
    best_j: Optional[int] = None
    best_fh: Optional[int] = None
    for j, vt in enumerate(valid_times):
        if vt is None:
            continue
        fh = fhr_row[j] if j < len(fhr_row) else None
        if fh is None or fh < 6 or fh > 48:
            continue
        if vt.astimezone(z).date() != target_date:
            continue
        if best_j is None or fh < best_fh:
            best_j = j
            best_fh = fh
    return best_j


def fetch_pctmax_from_nbp_text(
    station: str,
    valid_start_utc: datetime,
    nws_p50_f: float,
) -> tuple[float, float, float, dict[str, Any]]:
    """
    Download latest-available NBP bulletin; extract TXNP1/TXNP5/TXNP9 (°F) at best FHR column.

    TXNP* rows: NBM probabilistic daily max-T deciles (10th / 50th / 90th) in °F.

    Returns (p10, p50, p90, meta).
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


def nws_log_context(grid_url: str, target: date) -> dict[str, Any]:
    """Wind direction and sky cover at ~7am local on target (logging only, not used in model)."""
    grid = _http_json(grid_url)
    props = grid["properties"]
    z = _zone()
    t7 = datetime(target.year, target.month, target.day, 7, 0, tzinfo=z).astimezone(timezone.utc)
    ctx: dict[str, Any] = {"wind_dir_7am": None, "sky_cover_7am": None}
    for key, out_key in (("windDirection", "wind_dir_7am"), ("skyCover", "sky_cover_7am")):
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
) -> list[BracketRow]:
    """Core model: zones from biased NBM triple; optional live Kalshi list."""
    p10, p50, p90 = apply_nbm_bias(pct_f_raw[0], pct_f_raw[1], pct_f_raw[2], nbm_bias)
    z = build_zones(p10, p50, p90)
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


def fetch_live_nbm_fahrenheit(lat: float, lon: float, target: date) -> tuple[tuple[float, float, float], dict[str, Any]]:
    """NBP text TXNP1/5/9 in °F plus metadata."""
    grid_url = nws_grid_url_for_point(lat, lon)
    valid_start, nws_p50_f = nws_daily_max_period_for_date(grid_url, target)
    p10, p50, p90, meta = fetch_pctmax_from_nbp_text(NBP_STATION, valid_start, nws_p50_f)
    return (p10, p50, p90), meta


def synthetic_self_test() -> None:
    """Hardcoded checks before any live pull (project_docs gate)."""
    bias = 0.0
    z = build_zones(50.0, 60.0, 70.0)
    assert abs(zone_cdf(z, 60.0) - 0.5) < 1e-9
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

    rows = run_model(target, nbm_bias, args.series, pct_f)
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
        )
        print_terminal_review(target, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
