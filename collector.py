#!/usr/bin/env python3
"""
collector.py (v0 additive infra)

At each run, capture all currently open KXHIGH* event dates for all configured cities:
- pull open markets for each series ticker
- group by event_date parsed from ticker (e.g. 26MAR30)
- for each event_date, pull NBM percentiles for that city and compute model_prob
- log one snapshot per bracket with snapshot_type="intraday"

Cities are configured in config.json under the "cities" key.  Backward-compatible
with a single-city config that has only "nbm_bias" at the top level.

This does not change v0 evaluation gates: evaluation uses morning_model.py
snapshot_type="morning" rows only.
"""

from __future__ import annotations

import argparse
import csv
import html
import io
import json
import re
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, DefaultDict, Optional, Tuple

from intraday_engine import execute_paper_trades, generate_signals
from logger import (
    DEFAULT_DB_NAME,
    _forecast_lead_hours,
    latest_metar_observation_ts_utc,
    latest_metar_wind_speed_kt,
    latest_morning_overnight_and_record_high,
    running_observed_max_f,
    log_cli_observation,
    log_dsm_observation,
    log_metar_observations,
    log_morning_run,
)
from morning_model import (
    DEFAULT_KALSHI_SERIES,
    KNYC_LAT,
    KNYC_LON,
    NBP_STATION,
    BracketRow,
    ZoneModel,
    apply_ensemble_width,
    apply_trajectory_shift,
    bracket_prob,
    build_zones,
    combine_shifts,
    compute_trajectory_deviation,
    fetch_ensemble_spread,
    fetch_hrrr_forecast,
    fetch_live_nbm_fahrenheit,
    fetch_sunrise_sunset,
    hrrr_blend_weight_for_lead,
    kalshi_integration_bounds,
    kalshi_mid_price,
    load_config,
    truncate_and_renormalize,
)

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore


DSM_URL = "https://forecast.weather.gov/product.php?site=NWS&issuedby=NYC&product=DSM"
CLI_URL = "https://forecast.weather.gov/product.php?site=OKX&product=CLI&issuedby=NYC"
ASOS_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"


def _parse_event_date_from_ticker(ticker: str) -> date:
    """
    Parse Kalshi ticker token like 26MAR30 -> 2026-03-30.
    Expected: ...-YYMONDD-... where MON is 3-letter English month abbrev.
    """
    m = re.search(r"-([0-9]{2})([A-Z]{3})([0-9]{2})-", ticker)
    if not m:
        raise ValueError(f"Cannot parse event date from ticker: {ticker!r}")
    yy = int(m.group(1))
    mon = m.group(2)
    dd = int(m.group(3))
    mm_map = {
        "JAN": 1,
        "FEB": 2,
        "MAR": 3,
        "APR": 4,
        "MAY": 5,
        "JUN": 6,
        "JUL": 7,
        "AUG": 8,
        "SEP": 9,
        "OCT": 10,
        "NOV": 11,
        "DEC": 12,
    }
    if mon not in mm_map:
        raise ValueError(f"Unknown month token {mon!r} in ticker {ticker!r}")
    year = 2000 + yy
    return date(year, mm_map[mon], dd)


def fetch_open_kalshi_markets(series_ticker: str) -> list[dict[str, Any]]:
    """
    All currently open markets for a Kalshi series (no event-date filtering).
    Uses the same public trade API endpoint as morning_model.py.
    """
    from urllib.parse import urlencode

    from morning_model import _http_json

    qs = urlencode({"series_ticker": series_ticker, "status": "open"})
    url = f"https://api.elections.kalshi.com/trade-api/v2/markets?{qs}"
    data = _http_json(url)
    markets = data.get("markets", [])
    if not isinstance(markets, list):
        raise RuntimeError("Unexpected Kalshi markets payload")
    return markets


def _fetch_nws_product_text(url: str, timeout: int = 30) -> str:
    from urllib.request import Request, urlopen

    req = Request(url, headers={"User-Agent": "(nyc-temp-model, local)"})
    with urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="replace")
    m = re.search(r"<pre[^>]*>(.*?)</pre>", body, flags=re.IGNORECASE | re.DOTALL)
    if m:
        return html.unescape(m.group(1)).strip()
    return html.unescape(body).strip()


def _parse_issuance_ts_from_wmo(raw_text: str, fetch_ts_utc: datetime) -> Optional[datetime]:
    """
    Parse WMO ddhhmm header (e.g. SXUS71 KOKX 302121) into UTC datetime.
    Uses fetch_ts for month/year context.
    """
    m = re.search(r"\b[A-Z]{4}\d{2}\s+[A-Z]{4}\s+([0-9]{2})([0-9]{2})([0-9]{2})\b", raw_text)
    if not m:
        return None
    dd = int(m.group(1))
    hh = int(m.group(2))
    mm = int(m.group(3))
    ref = fetch_ts_utc.astimezone(timezone.utc)

    candidates = []
    for month_delta in (-1, 0, 1):
        y = ref.year
        mo = ref.month + month_delta
        while mo < 1:
            mo += 12
            y -= 1
        while mo > 12:
            mo -= 12
            y += 1
        try:
            cand = datetime(y, mo, dd, hh, mm, tzinfo=timezone.utc)
        except ValueError:
            continue
        candidates.append(cand)
    if not candidates:
        return None
    return min(candidates, key=lambda dt: abs((dt - ref).total_seconds()))


def _parse_dsm_observation(raw_text: str, fetch_ts_utc: datetime) -> Optional[Tuple[Optional[datetime], date, float]]:
    text = raw_text.upper()
    matches = [
        r"MAX(?:IMUM)?\s+TEMPERATURE[^0-9\-]*(-?\d{1,3})",
        r"HIGHEST(?:\s+TEMPERATURE)?[^0-9\-]*(-?\d{1,3})",
        r"TODAY(?:'S)?\s+HIGH[^0-9\-]*(-?\d{1,3})",
    ]
    high: Optional[float] = None
    for pat in matches:
        m = re.search(pat, text)
        if m:
            try:
                high = float(m.group(1))
                break
            except ValueError:
                continue

    # Compact DSM coded format, e.g.:
    # KNYC DS 30/03 731351/ 470340// 73/ 47//...
    ds_date: Optional[date] = None
    if high is None:
        m_ds = re.search(
            r"\bKNYC\s+DS\s+([0-9]{2})/([0-9]{2})\s+.*?//\s*(-?[0-9]{1,3})/\s*-?[0-9]{1,3}//",
            text,
            flags=re.DOTALL,
        )
        if m_ds:
            dd = int(m_ds.group(1))
            mm = int(m_ds.group(2))
            try:
                high = float(m_ds.group(3))
            except ValueError:
                high = None
            if high is not None:
                ref = fetch_ts_utc.astimezone(timezone.utc)
                # Choose nearest plausible year around current date.
                candidates: list[date] = []
                for yy in (ref.year - 1, ref.year, ref.year + 1):
                    try:
                        candidates.append(date(yy, mm, dd))
                    except ValueError:
                        continue
                if candidates:
                    ds_date = min(candidates, key=lambda d: abs((datetime(d.year, d.month, d.day, tzinfo=timezone.utc) - ref).total_seconds()))
    if high is None:
        return None

    issuance_ts = _parse_issuance_ts_from_wmo(raw_text, fetch_ts_utc)
    ny_tz = ZoneInfo("America/New_York")
    if ds_date is not None:
        event_date = ds_date
    else:
        event_dt = issuance_ts.astimezone(ny_tz) if issuance_ts else fetch_ts_utc.astimezone(ny_tz)
        event_date = event_dt.date()
    return issuance_ts, event_date, high


def _parse_cli_observation(raw_text: str, fetch_ts_utc: datetime) -> Optional[Tuple[Optional[datetime], date, float, bool]]:
    text = raw_text.upper()
    is_preliminary = "PRELIMINARY" in text
    cli_high: Optional[float] = None

    # Tabular MAXIMUM row — intraday/real-time collection only; not used for logger
    # backfill settlement (which uses IEM daily). Skips record-extremes "HIGHEST ..." lines.
    m_high = re.search(
        r"^\s*MAXIMUM\s+(-?\d{1,3})\s+\d{3,4}\s*(?:AM|PM)",
        text,
        flags=re.MULTILINE,
    )
    if m_high:
        try:
            cli_high = float(m_high.group(1))
        except ValueError:
            cli_high = None

    # Fallback row in "DAILY CLIMATE DATA" section.
    if cli_high is None:
        m_max = re.search(r"MAXIMUM\s+TEMPERATURE\s*\(F\)\s+(-?\d{1,3})\b", text)
        if m_max:
            try:
                cli_high = float(m_max.group(1))
            except ValueError:
                cli_high = None
    if cli_high is None:
        return None

    issuance_ts = _parse_issuance_ts_from_wmo(raw_text, fetch_ts_utc)
    ny_tz = ZoneInfo("America/New_York")
    local_dt = issuance_ts.astimezone(ny_tz) if issuance_ts else fetch_ts_utc.astimezone(ny_tz)

    # Final CLI for yesterday is typically published the next morning.
    if is_preliminary:
        event_date = local_dt.date()
    else:
        if "YESTERDAY" in text or local_dt.hour < 12:
            event_date = local_dt.date() - timedelta(days=1)
        else:
            event_date = local_dt.date()
    return issuance_ts, event_date, cli_high, is_preliminary


def _parse_utc_valid_ts(raw: str) -> Optional[datetime]:
    s = raw.strip()
    if not s:
        return None
    # Common Mesonet format: "YYYY-MM-DD HH:MM"
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _to_float(v: str) -> Optional[float]:
    s = (v or "").strip()
    if s in ("", "M", "NA", "null", "None"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _to_int(v: str) -> Optional[int]:
    s = (v or "").strip()
    if s in ("", "M", "NA", "null", "None"):
        return None
    try:
        return int(round(float(s)))
    except ValueError:
        return None


NOAA_METAR_URL = "https://aviationweather.gov/api/data/metar"


def _fetch_metar_observations(
    sts_utc: datetime,
    ets_utc: datetime,
    station: str = "KNYC",
) -> list[dict[str, Any]]:
    """Fetch METAR observations from NOAA Aviation Weather Center for any ICAO station."""
    import math
    from urllib.parse import urlencode
    from urllib.request import Request, urlopen

    if ets_utc.tzinfo is None:
        ets_utc = ets_utc.replace(tzinfo=timezone.utc)
    now_utc = datetime.now(timezone.utc)
    hours_back = math.ceil((now_utc - sts_utc).total_seconds() / 3600) + 1
    hours_back = max(hours_back, 1)

    params = {"ids": station, "format": "json", "hours": hours_back}
    url = f"{NOAA_METAR_URL}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": "(nyc-temp-model, local)"})
    with urlopen(req, timeout=60) as resp:
        records = json.loads(resp.read().decode("utf-8", errors="replace"))

    out: list[dict[str, Any]] = []
    for rec in records:
        report_time = rec.get("reportTime") or rec.get("receiptTime")
        if not report_time:
            continue
        obs_ts = datetime.fromisoformat(report_time.replace("Z", "+00:00")).astimezone(timezone.utc)
        if obs_ts < sts_utc or obs_ts > ets_utc:
            continue
        temp_c = rec.get("temp")
        tmpf = round(temp_c * 9 / 5 + 32, 1) if temp_c is not None else None
        out.append(
            {
                "observation_ts": obs_ts,
                "tmpf": tmpf,
                "wind_dir_deg": rec.get("wdir"),
                "wind_speed_kt": rec.get("wspd"),
                "sky_cover": rec.get("cover") or None,
            }
        )
    return out


def _fetch_knyc_metar_observations(sts_utc: datetime, ets_utc: datetime) -> list[dict[str, Any]]:
    """Backward-compatible wrapper; use _fetch_metar_observations(station=...) for new callers."""
    return _fetch_metar_observations(sts_utc, ets_utc, station="KNYC")


def _rows_for_event(
    event_date: date,
    nbm_bias: float,
    pct_f_raw: tuple[float, float, float],
    markets: list[dict[str, Any]],
    nbp_meta: Optional[dict[str, Any]] = None,
    adjusted_zone: Optional[ZoneModel] = None,
) -> tuple[list[BracketRow], ZoneModel, float]:
    """
    Compute model_prob for each bracket market for this event_date using shared zone math.

    If adjusted_zone is provided it is used for bracket probability computation instead of
    building the zone from pct_f_raw + nbm_bias.  The z_triplet comparison CDF is always
    built from the raw biased NBM percentiles so it remains an unmodified baseline.
    """
    meta = nbp_meta or {}
    p25r = meta.get("nbm_p25_raw")
    p75r = meta.get("nbm_p75_raw")
    try:
        p25f = float(p25r) if p25r is not None else None
    except (TypeError, ValueError):
        p25f = None
    try:
        p75f = float(p75r) if p75r is not None else None
    except (TypeError, ValueError):
        p75f = None
    p10b, p50b, p90b = pct_f_raw[0] + nbm_bias, pct_f_raw[1] + nbm_bias, pct_f_raw[2] + nbm_bias
    p25b = p25f + nbm_bias if p25f is not None else None
    p75b = p75f + nbm_bias if p75f is not None else None
    if p25b is None or p75b is None:
        import sys
        print(
            f"[collector] WARNING: p25/p75 not available for {event_date}, "
            f"falling back to 3-knot CDF. Check NBP bulletin for TXNP2/TXNP7.",
            file=sys.stderr,
        )
    z = adjusted_zone if adjusted_zone is not None else build_zones(p10b, p50b, p90b, p25b, p75b)
    # z_triplet is always built from raw biased percentiles — ensemble width does not touch it.
    z_triplet = build_zones(p10b, p50b, p90b) if (p25b is not None and p75b is not None) else None
    rows: list[BracketRow] = []
    for m in markets:
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
                model_prob=mp,
                edge=mp - price,
                market_bid=bid_f,
                market_ask=ask_f,
                model_prob_triplet_cdf=mp3,
            )
        )
    # Sort and sanity check via the same conventions as morning_model (when full partition present)
    rows.sort(key=lambda r: r.lower_f if r.lower_f != float("-inf") else -999)
    return rows, z, p50b


def main() -> int:
    ap = argparse.ArgumentParser(description="v0 collector (intraday snapshots; multi-city)")
    ap.add_argument("--series", type=str, default=DEFAULT_KALSHI_SERIES,
                    help="Override series ticker (ignored when config.json has cities list)")
    ap.add_argument("--config", type=Path, default=Path(__file__).resolve().parent / "config.json")
    ap.add_argument("--db", type=Path, default=Path(__file__).resolve().parent / DEFAULT_DB_NAME)
    args = ap.parse_args()

    snapshot_ts = datetime.now(timezone.utc)

    # Load config: cities list with per-city params (backward-compatible with single-city config)
    try:
        config_data = json.loads(args.config.read_text(encoding="utf-8"))
    except Exception:
        config_data = {}
    cities: list[dict[str, Any]] = config_data.get("cities", [])
    if not cities:
        # Backward compat: single NYC city derived from top-level nbm_bias
        top_bias = load_config(args.config)
        cities = [{
            "series_ticker": args.series,
            "station": NBP_STATION,
            "lat": KNYC_LAT,
            "lon": KNYC_LON,
            "nbm_bias": top_bias,
            "paper_trading": True,
        }]

    # DSM/CLI monitoring — NYC-specific; stays outside the city loop
    try:
        dsm_raw = _fetch_nws_product_text(DSM_URL)
        dsm = _parse_dsm_observation(dsm_raw, snapshot_ts)
        if dsm is not None:
            dsm_issuance_ts, dsm_event_date, dsm_high = dsm
            dsm_changed = log_dsm_observation(
                args.db,
                fetch_ts_utc=snapshot_ts,
                issuance_ts_utc=dsm_issuance_ts,
                event_date=dsm_event_date,
                running_high_f=dsm_high,
                raw_text=dsm_raw,
            )
            if dsm_changed:
                print(
                    f"collector: DSM running high changed for {dsm_event_date.isoformat()} -> {dsm_high}F"
                )
    except Exception:
        pass

    try:
        cli_raw = _fetch_nws_product_text(CLI_URL)
        cli = _parse_cli_observation(cli_raw, snapshot_ts)
        if cli is not None:
            cli_issuance_ts, cli_event_date, cli_high, cli_is_prelim = cli
            cli_first = log_cli_observation(
                args.db,
                fetch_ts_utc=snapshot_ts,
                issuance_ts_utc=cli_issuance_ts,
                event_date=cli_event_date,
                cli_high_f=cli_high,
                is_preliminary=cli_is_prelim,
                raw_text=cli_raw,
            )
            if cli_first and cli_event_date == snapshot_ts.astimezone(ZoneInfo("America/New_York")).date():
                print(
                    f"collector: first CLI high detected for {cli_event_date.isoformat()} -> {cli_high}F "
                    f"(preliminary={str(cli_is_prelim).lower()})"
                )
    except Exception:
        pass

    ny_tz = ZoneInfo("America/New_York")
    ny_today = snapshot_ts.astimezone(ny_tz).date()
    total_events = 0

    for city_cfg in cities:
        series_ticker = str(city_cfg["series_ticker"])
        station = str(city_cfg["station"])
        lat = float(city_cfg["lat"])
        lon = float(city_cfg["lon"])
        city_nbm_bias = float(city_cfg.get("nbm_bias", 0.0))
        paper_trading = bool(city_cfg.get("paper_trading", False))

        try:
            # METAR backfill for this city's station
            metar_new_obs = False
            try:
                today_utc = snapshot_ts.date()
                day_start_utc = datetime(today_utc.year, today_utc.month, today_utc.day, 6, 0, tzinfo=timezone.utc)
                prev_metar_ts = latest_metar_observation_ts_utc(args.db, today_utc, station=station)
                sts_utc = prev_metar_ts if prev_metar_ts is not None else day_start_utc
                if sts_utc < day_start_utc:
                    sts_utc = day_start_utc
                if sts_utc > snapshot_ts:
                    sts_utc = snapshot_ts - timedelta(hours=2)
                metar_rows = _fetch_metar_observations(sts_utc, snapshot_ts, station=station)
                inserted = log_metar_observations(args.db, metar_rows, snapshot_ts, station=station)
                metar_new_obs = inserted > 0
                print(
                    f"collector[{series_ticker}]: metar backfill "
                    f"{sts_utc.strftime('%Y-%m-%dT%H:%M:%SZ')} -> "
                    f"{snapshot_ts.strftime('%Y-%m-%dT%H:%M:%SZ')} "
                    f"rows={len(metar_rows)} inserted={inserted}"
                )
            except Exception:
                pass

            markets = fetch_open_kalshi_markets(series_ticker)
            by_event: DefaultDict[date, list[dict[str, Any]]] = defaultdict(list)
            for m in markets:
                tkr = str(m.get("ticker", ""))
                try:
                    d = _parse_event_date_from_ticker(tkr)
                except Exception:
                    continue
                by_event[d].append(m)
            if not by_event:
                print(f"collector[{series_ticker}]: no open markets with parseable event dates, skipping")
                continue

            for ev in sorted(by_event.keys()):
                pct_f_raw, nbp_meta = fetch_live_nbm_fahrenheit(lat, lon, ev, station=station)
                wkt = latest_metar_wind_speed_kt(args.db, snapshot_ts, station=station)
                olow, rhf = latest_morning_overnight_and_record_high(
                    args.db, ev, series_ticker=series_ticker
                )
                ens: dict[str, Any] = {}
                try:
                    ens = fetch_ensemble_spread(lat, lon, ev)
                except Exception:
                    pass

                # --- Signal C: Ensemble width modulation ---
                ensemble_width_ratio: Optional[float] = None
                nbm_spread_raw_val = pct_f_raw[2] - pct_f_raw[0]
                rows_base, z_nbm, nbm_p50_adj = _rows_for_event(
                    ev, city_nbm_bias, pct_f_raw, by_event[ev], nbp_meta=nbp_meta
                )
                z_adj, ensemble_width_ratio = apply_ensemble_width(
                    z_nbm, ens.get("ens_gefs_sd_f"), ens.get("ens_ecmwf_sd_f"), nbm_spread_raw_val
                )
                if z_adj is not z_nbm:
                    rows, z = _rows_for_event(
                        ev, city_nbm_bias, pct_f_raw, by_event[ev],
                        nbp_meta=nbp_meta, adjusted_zone=z_adj
                    )[:2]
                    print(
                        f"collector[{series_ticker}]: ensemble width {ev.isoformat()}: "
                        f"ratio={ensemble_width_ratio:.3f} "
                        f"L {z_nbm.L:.1f}->{z_adj.L:.1f}  U {z_nbm.U:.1f}->{z_adj.U:.1f}"
                    )
                else:
                    rows, z = rows_base, z_nbm

                # --- Signal B: HRRR shift ---
                hrrr_max_f: Optional[float] = None
                hrrr_shift_applied_f: Optional[float] = None
                hrrr_blend_weight_val: Optional[float] = None
                hrrr_shift_component: Optional[float] = None
                forecast_lead_hours_val: Optional[float] = None
                shift_raw_hrrr: Optional[float] = None
                if ev in (ny_today, ny_today + timedelta(days=1)):
                    hrrr = fetch_hrrr_forecast(lat, lon, ev, as_of_utc=snapshot_ts)
                    if hrrr and hrrr.get("hrrr_max_f") is not None:
                        try:
                            hrrr_max_f = float(hrrr["hrrr_max_f"])
                        except (TypeError, ValueError):
                            pass
                if hrrr_max_f is not None:
                    forecast_lead_hours_val = _forecast_lead_hours(snapshot_ts, ev)
                    blend_weight = hrrr_blend_weight_for_lead(forecast_lead_hours_val)
                    hrrr_blend_weight_val = blend_weight
                    shift_raw_hrrr = hrrr_max_f - float(nbm_p50_adj)
                    if blend_weight > 0.0 and abs(shift_raw_hrrr) >= 1.0:  # PROVISIONAL
                        hrrr_shift_component = shift_raw_hrrr * blend_weight
                        hrrr_shift_applied_f = hrrr_shift_component
                    else:
                        hrrr_shift_applied_f = 0.0

                # --- Signal D: Trajectory deviation (today only) ---
                trajectory_deviation_f: Optional[float] = None
                trajectory_confidence: Optional[float] = None
                trajectory_shift_component: Optional[float] = None
                _traj_lead = (
                    forecast_lead_hours_val
                    if forecast_lead_hours_val is not None
                    else _forecast_lead_hours(snapshot_ts, ev)
                )
                if ev == ny_today and _traj_lead > 0.0:
                    if olow is not None:
                        try:
                            sunrise_utc, _ = fetch_sunrise_sunset(lat, lon, ev)
                            dev, conf = compute_trajectory_deviation(
                                args.db, ev, float(nbm_p50_adj), float(olow),
                                sunrise_utc, snapshot_ts, station=station,
                            )
                            trajectory_deviation_f = dev
                            trajectory_confidence = conf
                        except Exception:
                            pass
                        if trajectory_deviation_f is not None and trajectory_confidence is not None:
                            trajectory_shift_component = trajectory_deviation_f * trajectory_confidence
                    else:
                        print(
                            f"collector[{series_ticker}]: trajectory skipped {ev.isoformat()}: "
                            "overnight_low_f unavailable"
                        )

                # --- Combine and apply shifts ---
                combined_shift_f: Optional[float] = None
                if hrrr_shift_component is not None and trajectory_shift_component is not None:
                    combined_shift_f = combine_shifts(hrrr_shift_component, trajectory_shift_component)
                    rows = apply_trajectory_shift(z, combined_shift_f, 1.0, rows)
                    print(
                        f"collector[{series_ticker}]: combined shift {ev.isoformat()}: "
                        f"hrrr={hrrr_shift_component:+.2f}F traj={trajectory_shift_component:+.2f}F "
                        f"combined={combined_shift_f:+.2f}F (lead={forecast_lead_hours_val:.1f}h)"
                    )
                elif hrrr_shift_component is not None:
                    combined_shift_f = hrrr_shift_component
                    rows = apply_trajectory_shift(z, hrrr_shift_component, 1.0, rows)
                    print(
                        f"collector[{series_ticker}]: HRRR shift {ev.isoformat()}: "
                        f"hrrr_max={hrrr_max_f:.1f}F nbm_p50={nbm_p50_adj:.1f}F "
                        f"raw={shift_raw_hrrr:+.1f}F applied={hrrr_shift_component:+.2f}F "
                        f"(lead={forecast_lead_hours_val:.1f}h blend={hrrr_blend_weight_val:.1f})"
                    )
                elif trajectory_shift_component is not None:
                    combined_shift_f = trajectory_shift_component
                    rows = apply_trajectory_shift(z, trajectory_deviation_f, trajectory_confidence, rows)
                    print(
                        f"collector[{series_ticker}]: trajectory shift {ev.isoformat()}: "
                        f"dev={trajectory_deviation_f:+.2f}F conf={trajectory_confidence:.2f} "
                        f"shift={trajectory_shift_component:+.2f}F"
                    )

                # --- Signal A: Truncation — always last ---
                observed_max_f_at_snapshot: Optional[float] = None
                if ev == ny_today:
                    try:
                        observed_max_f_at_snapshot = running_observed_max_f(
                            args.db, ev, snapshot_ts, station=station
                        )
                    except Exception:
                        observed_max_f_at_snapshot = None
                    if observed_max_f_at_snapshot is not None:
                        before = sum(1 for r in rows if r.model_prob == 0.0)
                        rows = truncate_and_renormalize(rows, observed_max_f_at_snapshot)
                        after = sum(1 for r in rows if r.model_prob == 0.0)
                        print(
                            f"collector[{series_ticker}]: truncation {ev.isoformat()}: "
                            f"observed_max={observed_max_f_at_snapshot:.1f}F, "
                            f"{max(0, after - before)} brackets zeroed"
                        )

                # --- Paper signals (today only, gated on paper_trading flag) ---
                if paper_trading and ev == ny_today:
                    try:
                        signals = generate_signals(
                            ev, rows, args.db, snapshot_ts, observed_max_f_at_snapshot,
                            hrrr_shift_f=hrrr_shift_applied_f,
                            trajectory_deviation_f=trajectory_deviation_f,
                            ensemble_ratio=ensemble_width_ratio,
                            forecast_lead_hours=forecast_lead_hours_val,
                        )
                        n_changed = execute_paper_trades(signals, args.db)
                        if signals:
                            print(
                                f"collector[{series_ticker}]: signals {ev.isoformat()}: "
                                f"{len(signals)} generated, {n_changed} position(s) changed"
                            )
                    except Exception as _e:
                        print(f"collector[{series_ticker}]: intraday_engine error: {_e}")

                log_morning_run(
                    args.db,
                    ev,
                    rows,
                    pct_f_raw,
                    city_nbm_bias,
                    record_prox_flag=False,
                    nws_log_context={
                        "wind_speed_kt": wkt,
                        "overnight_low_f": olow,
                        "record_high_f": rhf,
                    },
                    snapshot_ts_utc=snapshot_ts,
                    snapshot_type="intraday",
                    nbp_meta=nbp_meta,
                    records_path=Path(__file__).resolve().parent / "records.json",
                    ensemble_snap=ens,
                    observed_max_f_at_snapshot=observed_max_f_at_snapshot,
                    hrrr_max_f=hrrr_max_f,
                    hrrr_shift_applied_f=hrrr_shift_applied_f,
                    metar_new_obs=metar_new_obs,
                    trajectory_deviation_f=trajectory_deviation_f,
                    trajectory_confidence=trajectory_confidence,
                    ensemble_width_ratio=ensemble_width_ratio,
                    combined_shift_f=combined_shift_f,
                    hrrr_blend_weight=hrrr_blend_weight_val,
                    series_ticker=series_ticker,
                )

            total_events += len(by_event)
            print(
                f"collector[{series_ticker}]: wrote {len(by_event)} event_date(s)"
            )

        except Exception as city_err:
            print(f"collector[{series_ticker}]: city failed: {city_err}")

    print(
        f"collector: done at {snapshot_ts.strftime('%Y-%m-%dT%H:%M:%SZ')} — "
        f"{len(cities)} city/cities, {total_events} event_date(s) total"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

