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
import re
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, DefaultDict

from logger import DEFAULT_DB_NAME, log_morning_run
from morning_model import (
    DEFAULT_KALSHI_SERIES,
    KNYC_LAT,
    KNYC_LON,
    BracketRow,
    bracket_prob,
    build_zones,
    fetch_live_nbm_fahrenheit,
    kalshi_integration_bounds,
    kalshi_mid_price,
    load_config,
)


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


def _rows_for_event(
    event_date: date,
    nbm_bias: float,
    pct_f_raw: tuple[float, float, float],
    markets: list[dict[str, Any]],
) -> list[BracketRow]:
    """
    Compute model_prob for each bracket market for this event_date using shared zone math.
    """
    p10b, p50b, p90b = pct_f_raw[0] + nbm_bias, pct_f_raw[1] + nbm_bias, pct_f_raw[2] + nbm_bias
    z = build_zones(p10b, p50b, p90b)
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
        rows = _rows_for_event(ev, nbm_bias, pct_f_raw, by_event[ev])
        # NWS context is currently sourced in morning_model; collector keeps it empty for now.
        log_morning_run(
            args.db,
            ev,
            rows,
            pct_f_raw,
            nbm_bias,
            record_prox_flag=False,
            nws_log_context={},
            snapshot_ts_utc=snapshot_ts,
            snapshot_type="intraday",
            nbp_meta=nbp_meta,
            records_path=Path(__file__).resolve().parent / "records.json",
        )

    print(f"collector: wrote intraday snapshots at {snapshot_ts.isoformat()} for {len(by_event)} event_date(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

