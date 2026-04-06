#!/usr/bin/env python3
"""
collector.py (v0 additive infra)

At each run, capture *all* currently open KXHIGHNY event dates:
- pull open markets for the series (KXHIGHNY)
- group by event_date parsed from ticker (e.g. 26MAR30)
- for each event_date, pull NBM percentiles appropriate to that date and compute model_prob
- log one snapshot per bracket with snapshot_type="intraday"

This does not change v0 evaluation gates: evaluation uses morning_model.py
snapshot_type="morning" rows only.
"""

from __future__ import annotations

import argparse
import csv
import html
import io
import re
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, DefaultDict, Optional, Tuple

from logger import (
    DEFAULT_DB_NAME,
    latest_metar_observation_ts_utc,
    latest_metar_wind_speed_kt,
    latest_morning_overnight_and_record_high,
    log_cli_observation,
    log_dsm_observation,
    log_metar_observations,
    log_morning_run,
)
from morning_model import (
    DEFAULT_KALSHI_SERIES,
    KNYC_LAT,
    KNYC_LON,
    BracketRow,
    bracket_prob,
    build_zones,
    fetch_ensemble_spread,
    fetch_live_nbm_fahrenheit,
    kalshi_integration_bounds,
    kalshi_mid_price,
    load_config,
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


def _fetch_knyc_metar_observations(sts_utc: datetime, ets_utc: datetime) -> list[dict[str, Any]]:
    from urllib.parse import urlencode
    from urllib.request import Request, urlopen

    params = [
        ("station", "KNYC"),
        ("data", "tmpf"),
        ("data", "drct"),
        ("data", "sknt"),
        ("data", "skyc1"),
        ("tz", "UTC"),
        ("format", "onlycomma"),
        ("sts", sts_utc.strftime("%Y-%m-%dT%H:%M:%SZ")),
        ("ets", ets_utc.strftime("%Y-%m-%dT%H:%M:%SZ")),
    ]
    url = f"{ASOS_URL}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": "(nyc-temp-model, local)"})
    with urlopen(req, timeout=60) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    if raw.lstrip().startswith("["):
        return []

    out: list[dict[str, Any]] = []
    rdr = csv.DictReader(io.StringIO(raw))
    for rec in rdr:
        t_raw = rec.get("valid") or rec.get("valid(UTC)") or ""
        obs_ts = _parse_utc_valid_ts(t_raw)
        if obs_ts is None:
            continue
        out.append(
            {
                "observation_ts": obs_ts,
                "tmpf": _to_float(rec.get("tmpf", "")),
                "wind_dir_deg": _to_int(rec.get("drct", "")),
                "wind_speed_kt": _to_int(rec.get("sknt", "")),
                "sky_cover": (rec.get("skyc1") or "").strip() or None,
            }
        )
    return out


def _rows_for_event(
    event_date: date,
    nbm_bias: float,
    pct_f_raw: tuple[float, float, float],
    markets: list[dict[str, Any]],
    nbp_meta: Optional[dict[str, Any]] = None,
) -> list[BracketRow]:
    """
    Compute model_prob for each bracket market for this event_date using shared zone math.
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
    z = build_zones(p10b, p50b, p90b, p25b, p75b)
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
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="v0 collector (intraday snapshots; multi-event capture)")
    ap.add_argument("--series", type=str, default=DEFAULT_KALSHI_SERIES)
    ap.add_argument("--config", type=Path, default=Path(__file__).resolve().parent / "config.json")
    ap.add_argument("--db", type=Path, default=Path(__file__).resolve().parent / DEFAULT_DB_NAME)
    args = ap.parse_args()

    snapshot_ts = datetime.now(timezone.utc)
    nbm_bias = load_config(args.config)

    # KNYC METAR 5-minute capture via 30-minute backfill gap pull.
    try:
        today_utc = snapshot_ts.date()
        day_start_utc = datetime(today_utc.year, today_utc.month, today_utc.day, 6, 0, tzinfo=timezone.utc)
        last_obs = latest_metar_observation_ts_utc(args.db, today_utc, station="KNYC")
        sts_utc = last_obs if last_obs is not None else day_start_utc
        if sts_utc < day_start_utc:
            sts_utc = day_start_utc
        if sts_utc > snapshot_ts:
            # day_start_utc is in the future (overnight cron window, new UTC date, no obs yet)
            sts_utc = snapshot_ts - timedelta(hours=2)
        metar_rows = _fetch_knyc_metar_observations(sts_utc, snapshot_ts)
        inserted = log_metar_observations(args.db, metar_rows, snapshot_ts, station="KNYC")
        print(
            f"collector: metar backfill {sts_utc.strftime('%Y-%m-%dT%H:%M:%SZ')} -> "
            f"{snapshot_ts.strftime('%Y-%m-%dT%H:%M:%SZ')} rows={len(metar_rows)} inserted={inserted}"
        )
    except Exception:
        # Observational feed should not block core collector run.
        pass

    # Observational feeds for information timing analysis (not model inputs).
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
        # Skip silently; retry next cycle.
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
        # Skip silently; retry next cycle.
        pass

    markets = fetch_open_kalshi_markets(args.series)
    by_event: DefaultDict[date, list[dict[str, Any]]] = defaultdict(list)
    for m in markets:
        tkr = str(m.get("ticker", ""))
        try:
            d = _parse_event_date_from_ticker(tkr)
        except Exception:
            continue
        by_event[d].append(m)
    if not by_event:
        raise SystemExit(f"No open {args.series} markets with parseable event dates.")

    # For each open event_date, compute the appropriate NBM triple for that date.
    for ev in sorted(by_event.keys()):
        pct_f_raw, nbp_meta = fetch_live_nbm_fahrenheit(KNYC_LAT, KNYC_LON, ev)
        rows = _rows_for_event(ev, nbm_bias, pct_f_raw, by_event[ev], nbp_meta=nbp_meta)
        # Intraday wind_speed_kt is METAR-derived (latest obs ≤ snapshot); morning_model uses NWS grid — different sources.
        wkt = latest_metar_wind_speed_kt(args.db, snapshot_ts, station="KNYC")
        olow, rhf = latest_morning_overnight_and_record_high(args.db, ev)
        ens: dict[str, Any] = {}
        try:
            ens = fetch_ensemble_spread(KNYC_LAT, KNYC_LON, ev)
        except Exception:
            pass
        log_morning_run(
            args.db,
            ev,
            rows,
            pct_f_raw,
            nbm_bias,
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
        )

    print(f"collector: wrote intraday snapshots at {snapshot_ts.isoformat()} for {len(by_event)} event_date(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

