#!/usr/bin/env python3
"""
v0 SQLite log.

Key concepts:
- event_date: which day's high temp the market settles on
- snapshot_ts: when this snapshot was captured (UTC)
- snapshot_type: "morning" (7am run) vs "intraday" (collector)

Invoked from morning_model after each run. Terminal review on stderr
(stdout stays JSON).
"""

from __future__ import annotations

import math
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore


DEFAULT_DB_NAME = "nyc_temp_log.sqlite"

SCHEMA = """
CREATE TABLE IF NOT EXISTS bracket_snapshots (
    event_date TEXT NOT NULL,
    snapshot_ts TEXT NOT NULL,
    snapshot_type TEXT NOT NULL,

    bracket_label TEXT NOT NULL,
    bracket_lower_f REAL,
    bracket_upper_f REAL,

    model_prob REAL,
    market_price REAL,
    market_bid REAL,
    market_ask REAL,
    edge REAL,
    outcome INTEGER,

    actual_max_f REAL,

    nbm_p10_raw REAL,
    nbm_p50_raw REAL,
    nbm_p90_raw REAL,
    nbm_bias_applied REAL,
    nbm_p10_adj REAL,
    nbm_p50_adj REAL,
    nbm_p90_adj REAL,
    nbm_spread_raw REAL,
    nbm_cycle TEXT,
    forecast_lead_hours REAL,

    wind_dir TEXT,
    wind_speed_kt INTEGER,
    sky_cover TEXT,
    overnight_low_f REAL,
    record_high_f REAL,
    record_prox_flag INTEGER,

    market_open_ts TEXT,
    hours_since_open REAL,
    hours_to_settle REAL,

    PRIMARY KEY (event_date, snapshot_ts, snapshot_type, bracket_label)
);
"""


def _bounds_db(lower_f: float, upper_f: float) -> Tuple[Optional[float], Optional[float]]:
    lo = None if math.isinf(lower_f) and lower_f < 0 else float(lower_f)
    hi = None if math.isinf(upper_f) and upper_f > 0 else float(upper_f)
    return lo, hi


def bracket_label(lower_f: float, upper_f: float) -> str:
    """Short label for SQLite + terminal (e.g. 59-60, >75, <40)."""
    if math.isinf(lower_f) and lower_f < 0:
        assert not (math.isinf(upper_f) and upper_f < 0)
        u = upper_f
        return f"<{u:g}"
    if math.isinf(upper_f) and upper_f > 0:
        lo = lower_f
        return f">{lo:g}"
    a, b = lower_f, upper_f
    if a == int(a) and b == int(b):
        return f"{int(a)}-{int(b)}"
    return f"{a:g}-{b:g}"


def _record_high_for_date(target: date, records_path: Optional[Path]) -> Optional[float]:
    if not records_path or not records_path.is_file():
        return None
    try:
        import json

        data = json.loads(records_path.read_text(encoding="utf-8"))
        key = target.strftime("%m%d")
        rec = data.get(key)
        if not rec or "record_high_f" not in rec:
            return None
        return float(rec["record_high_f"])
    except (OSError, ValueError, TypeError, KeyError):
        return None


def _wind_sky_from_nws(ctx: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    wd = ctx.get("wind_dir_7am")
    sc = ctx.get("sky_cover_7am")
    if wd is not None and not isinstance(wd, str):
        wd = str(wd)
    if sc is not None and not isinstance(sc, str):
        sc = str(sc)
    return wd, sc


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def _utc_z(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _infer_market_open_ts_utc(event_date: date, tz_name: str = "America/New_York") -> datetime:
    """
    Spec assumption: Kalshi temp markets open at ~10:00 local on the day before event_date.
    If the API later provides an official open timestamp, prefer that.
    """
    z = ZoneInfo(tz_name)
    local = datetime(event_date.year, event_date.month, event_date.day, 10, 0, tzinfo=z) - timedelta(days=1)
    return local.astimezone(timezone.utc)


def _forecast_lead_hours(snapshot_ts_utc: datetime, event_date: date, tz_name: str = "America/New_York") -> float:
    """Hours between snapshot_ts and ~14:00 local peak time on event_date (PROVISIONAL peak time)."""
    z = ZoneInfo(tz_name)
    peak_local = datetime(event_date.year, event_date.month, event_date.day, 14, 0, tzinfo=z)
    return (peak_local.astimezone(timezone.utc) - snapshot_ts_utc.astimezone(timezone.utc)).total_seconds() / 3600.0


def _hours_to_settle(snapshot_ts_utc: datetime, event_date: date, tz_name: str = "America/New_York") -> float:
    """Approx hours between snapshot_ts and expected CLI release (~18:00 local on event_date; PROVISIONAL)."""
    z = ZoneInfo(tz_name)
    settle_local = datetime(event_date.year, event_date.month, event_date.day, 18, 0, tzinfo=z)
    return (settle_local.astimezone(timezone.utc) - snapshot_ts_utc.astimezone(timezone.utc)).total_seconds() / 3600.0


def _nbm_cycle_from_meta(nbp_meta: Dict[str, Any]) -> Optional[str]:
    raw = nbp_meta.get("nbp_cycle_init_utc")
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%HZ")
    except Exception:
        return str(raw)


def log_morning_run(
    db_path: Path,
    event_date: date,
    rows: List[Any],
    pct_f_raw: Tuple[float, float, float],
    nbm_bias: float,
    record_prox_flag: bool,
    nws_log_context: Dict[str, Any],
    snapshot_ts_utc: datetime,
    snapshot_type: str = "morning",
    nbp_meta: Optional[Dict[str, Any]] = None,
    records_path: Optional[Path] = None,
) -> None:
    """Insert one snapshot (one row per bracket) for a single event_date."""
    p10, p50, p90 = pct_f_raw
    nbm_spread_raw = p90 - p10
    p10a, p50a, p90a = p10 + nbm_bias, p50 + nbm_bias, p90 + nbm_bias
    wind_dir, sky_cover = _wind_sky_from_nws(nws_log_context)
    rh = _record_high_for_date(event_date, records_path)
    nbm_cycle = _nbm_cycle_from_meta(nbp_meta or {})

    if snapshot_ts_utc.tzinfo is None:
        snapshot_ts_utc = snapshot_ts_utc.replace(tzinfo=timezone.utc)
    snapshot_s = _utc_z(snapshot_ts_utc)

    open_ts_utc = _infer_market_open_ts_utc(event_date)
    open_s = _utc_z(open_ts_utc)
    hours_since_open = (snapshot_ts_utc.astimezone(timezone.utc) - open_ts_utc).total_seconds() / 3600.0
    forecast_lead_hours = _forecast_lead_hours(snapshot_ts_utc, event_date)
    hours_to_settle = _hours_to_settle(snapshot_ts_utc, event_date)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        ensure_schema(conn)
        for r in rows:
            label = getattr(r, "bracket_label", None) or bracket_label(r.lower_f, r.upper_f)
            lo_db, hi_db = _bounds_db(r.lower_f, r.upper_f)
            bid = getattr(r, "market_bid", None)
            ask = getattr(r, "market_ask", None)
            conn.execute(
                """
                INSERT OR REPLACE INTO bracket_snapshots (
                    event_date, snapshot_ts, snapshot_type,
                    bracket_label, bracket_lower_f, bracket_upper_f,
                    model_prob, market_price, market_bid, market_ask, edge, outcome,
                    actual_max_f,
                    nbm_p10_raw, nbm_p50_raw, nbm_p90_raw, nbm_bias_applied,
                    nbm_p10_adj, nbm_p50_adj, nbm_p90_adj, nbm_spread_raw,
                    nbm_cycle, forecast_lead_hours,
                    wind_dir, wind_speed_kt, sky_cover, overnight_low_f,
                    record_high_f, record_prox_flag,
                    market_open_ts, hours_since_open, hours_to_settle
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    event_date.isoformat(),
                    snapshot_s,
                    snapshot_type,
                    label,
                    lo_db,
                    hi_db,
                    r.model_prob,
                    r.market_price,
                    bid,
                    ask,
                    r.edge,
                    None,
                    None,
                    p10,
                    p50,
                    p90,
                    nbm_bias,
                    p10a,
                    p50a,
                    p90a,
                    nbm_spread_raw,
                    nbm_cycle,
                    forecast_lead_hours,
                    wind_dir,
                    None,
                    sky_cover,
                    None,
                    rh,
                    1 if record_prox_flag else 0,
                    open_s,
                    hours_since_open,
                    hours_to_settle,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def print_terminal_review(target: date, rows: List[Any], file: Any = None) -> None:
    """Human-readable bracket table; outcome column reserved for backfill (shown as —)."""
    import sys

    out = file if file is not None else sys.stderr
    lines = [
        f"[logger] {target.isoformat()}  model_prob vs market_price vs outcome (outcome backfilled later)",
        f"{'label':<12} {'p_model':>8} {'p_mkt':>8} {'edge':>8} {'outcome':>8}",
        "-" * 52,
    ]
    for r in rows:
        lab = getattr(r, "bracket_label", None) or bracket_label(r.lower_f, r.upper_f)
        lines.append(
            f"{lab:<12} {r.model_prob:8.4f} {r.market_price:8.4f} {r.edge:8.4f} {'—':>8}"
        )
    print("\n".join(lines), file=out)


def backfill_outcome(
    db_path: Path,
    event_date: date,
    actual_max_f: float,
) -> int:
    """
    Update all bracket_snapshots rows for event_date with the verified
    actual high temperature and computed outcome.

    Sets:
    - actual_max_f = the NWS CLI value for every row with this event_date
    - outcome = 1 if actual_max_f falls within [bracket_lower_f, bracket_upper_f),
                0 otherwise
      For the low tail bracket (bracket_lower_f is NULL): outcome = 1 if
        actual_max_f < bracket_upper_f
      For the high tail bracket (bracket_upper_f is NULL): outcome = 1 if
        actual_max_f >= bracket_lower_f
      For middle brackets: outcome = 1 if
        bracket_lower_f <= actual_max_f < bracket_upper_f

    Note: brackets use the half-degree continuity correction, so
    bracket_lower_f=61.5 and bracket_upper_f=63.5 means the bracket
    covers integer outcomes 62 and 63. Since actual_max_f from the CLI
    is always an integer, the comparison works correctly with < on the
    upper bound.

    Returns the number of rows updated.
    """
    import sys

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        ensure_schema(conn)
        cur = conn.execute(
            """
            SELECT DISTINCT bracket_label, bracket_lower_f, bracket_upper_f
            FROM bracket_snapshots
            WHERE event_date = ?
            """,
            (event_date.isoformat(),),
        )
        brackets = list(cur.fetchall())
        if not brackets:
            return 0

        outcomes: List[Tuple[str, int]] = []
        for label, lo, hi in brackets:
            lo_f = float(lo) if lo is not None else None
            hi_f = float(hi) if hi is not None else None
            if lo_f is None and hi_f is None:
                is_win = False
            elif lo_f is None:
                is_win = actual_max_f < hi_f  # type: ignore[operator]
            elif hi_f is None:
                is_win = actual_max_f >= lo_f
            else:
                is_win = (lo_f <= actual_max_f) and (actual_max_f < hi_f)
            outcomes.append((str(label), 1 if is_win else 0))

        winners = sum(o for _, o in outcomes)
        if winners != 1:
            print(
                f"WARNING: backfill outcome winners={winners} for event_date={event_date.isoformat()} "
                f"(actual_max_f={actual_max_f}). Check bracket boundaries / continuity correction.",
                file=sys.stderr,
            )

        updated = 0
        for label, outc in outcomes:
            cur2 = conn.execute(
                """
                UPDATE bracket_snapshots
                SET actual_max_f = ?, outcome = ?
                WHERE event_date = ? AND bracket_label = ?
                """,
                (float(actual_max_f), int(outc), event_date.isoformat(), label),
            )
            updated += int(cur2.rowcount or 0)
        conn.commit()
        return updated
    finally:
        conn.close()


def fetch_knyc_cli_max(target_date: date) -> float:
    """
    Fetch the verified daily maximum temperature (°F) for KNYC from
    the NWS Daily Climate Report.

    Uses the Iowa State Mesonet CLI archive:
    https://mesonet.agron.iastate.edu/cgi-bin/request/daily.py

    Falls back to ASOS hourly max if CLI is not yet available.

    Raises RuntimeError if no data is available for the target date.
    """
    import csv
    import io
    import sys
    from urllib.parse import urlencode
    from urllib.request import Request, urlopen

    DAILY_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/daily.py"
    station = "KNYC"
    network = "NY_ASOS"
    params = {
        "station": station,
        "network": network,
        "year": target_date.year,
        "month": target_date.month,
        "day": target_date.day,
        "format": "csv",
    }
    url = f"{DAILY_URL}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": "(nyc-temp-model, local)"})
    try:
        with urlopen(req, timeout=60) as resp:
            text = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        raise RuntimeError(f"Failed to fetch NWS daily climate report for {target_date}: {e}") from e

    buf = io.StringIO(text)
    reader = csv.DictReader(buf)
    rows = list(reader)

    def _pick_max_key(keys: List[str]) -> Optional[str]:
        # Common IEM daily fields (vary by endpoint / config). Keep this permissive.
        candidates = [
            "max_tmpf",
            "max_temp_f",
            "max_temp",
            "high",
            "high_f",
            "max",
        ]
        keyset = {k.strip().lower(): k for k in keys}
        for c in candidates:
            if c in keyset:
                return keyset[c]
        for k in keys:
            kl = k.strip().lower()
            if ("max" in kl or "high" in kl) and ("tmp" in kl or "temp" in kl) and ("f" in kl):
                return k
        return None

    if rows:
        key = _pick_max_key(list(rows[0].keys()))
        if key is not None:
            raw = (rows[0].get(key) or "").strip()
            if raw not in ("", "M", "NA", "None", "null"):
                try:
                    return float(raw)
                except ValueError:
                    pass

    # If the daily report isn't published yet, try an ASOS-hourly fallback for completed past dates.
    # This fallback is explicitly *not* silent.
    if target_date >= date.today():
        raise RuntimeError(
            f"No daily climate report data available yet for {target_date}. "
            "This is common before ~7am ET for yesterday's report; re-run later or use --actual-max."
        )

    try:
        from urllib.error import URLError

        ASOS_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
        tz = ZoneInfo("America/New_York")
        sts = datetime(target_date.year, target_date.month, target_date.day, 0, 0, tzinfo=tz).astimezone(timezone.utc)
        ets = sts + timedelta(days=1)
        p2 = {
            "station": station,
            "data": "tmpf",
            "year1": sts.year,
            "month1": sts.month,
            "day1": sts.day,
            "year2": ets.year,
            "month2": ets.month,
            "day2": ets.day,
            "tz": "UTC",
            "format": "onlycomma",
        }
        url2 = f"{ASOS_URL}?{urlencode(p2)}"
        req2 = Request(url2, headers={"User-Agent": "(nyc-temp-model, local)"})
        with urlopen(req2, timeout=60) as resp2:
            text2 = resp2.read().decode("utf-8", errors="replace")
        rdr2 = csv.DictReader(io.StringIO(text2))
        vals: List[float] = []
        for r in rdr2:
            v = (r.get("tmpf") or "").strip()
            if v in ("", "M", "NA"):
                continue
            try:
                vals.append(float(v))
            except ValueError:
                continue
        if not vals:
            raise RuntimeError("ASOS hourly fallback returned no tmpf values")
        mx = max(vals)
        print(
            f"WARNING: daily climate report unavailable; using ASOS hourly max fallback for {target_date}: {mx}",
            file=sys.stderr,
        )
        return float(mx)
    except Exception as e:
        raise RuntimeError(
            f"No daily climate report data available for {target_date}, and ASOS fallback failed: {e}. "
            "Re-run later or use --actual-max."
        ) from e


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="Backfill actual_max_f and outcome for a past event_date"
    )
    parser.add_argument(
        "event_date",
        type=str,
        nargs="?",
        help="YYYY-MM-DD to backfill (default: yesterday)",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(__file__).resolve().parent / DEFAULT_DB_NAME,
    )
    parser.add_argument(
        "--actual-max",
        type=float,
        default=None,
        help="Manually specify actual max °F instead of pulling from NWS",
    )
    args = parser.parse_args()

    if args.event_date:
        target = date.fromisoformat(args.event_date)
    else:
        target = date.today() - timedelta(days=1)

    if args.actual_max is not None:
        actual = args.actual_max
        print(f"Using manually specified actual_max_f: {actual}")
    else:
        print(f"Fetching NWS CLI actual max for KNYC on {target}...")
        try:
            actual = fetch_knyc_cli_max(target)
        except RuntimeError as e:
            print(str(e), file=sys.stderr)
            raise SystemExit(2)
        print(f"NWS CLI actual_max_f: {actual}")

    n = backfill_outcome(args.db, target, actual)
    print(f"Updated {n} rows for event_date={target}")
