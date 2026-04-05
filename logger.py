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

from morning_model import kalshi_settlement_wins

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

CREATE TABLE IF NOT EXISTS dsm_observations (
    fetch_ts TEXT NOT NULL,
    issuance_ts TEXT,
    event_date TEXT NOT NULL,
    running_high_f REAL NOT NULL,
    raw_text TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cli_observations (
    fetch_ts TEXT NOT NULL,
    issuance_ts TEXT,
    event_date TEXT NOT NULL,
    cli_high_f REAL NOT NULL,
    is_preliminary INTEGER NOT NULL,
    raw_text TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS metar_observations (
    observation_ts TEXT NOT NULL,
    station TEXT NOT NULL DEFAULT 'KNYC',
    tmpf REAL,
    wind_dir_deg INTEGER,
    wind_speed_kt INTEGER,
    sky_cover TEXT,
    fetch_ts TEXT NOT NULL,
    PRIMARY KEY (observation_ts, station)
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


def log_dsm_observation(
    db_path: Path,
    fetch_ts_utc: datetime,
    issuance_ts_utc: Optional[datetime],
    event_date: date,
    running_high_f: float,
    raw_text: str,
) -> bool:
    """
    Insert one DSM observation row.
    Returns True when running_high_f changed versus the prior row for event_date.
    """
    if fetch_ts_utc.tzinfo is None:
        fetch_ts_utc = fetch_ts_utc.replace(tzinfo=timezone.utc)
    fetch_s = _utc_z(fetch_ts_utc)
    issuance_s = _utc_z(issuance_ts_utc) if issuance_ts_utc is not None else None

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        ensure_schema(conn)
        prev = conn.execute(
            """
            SELECT running_high_f
            FROM dsm_observations
            WHERE event_date = ?
            ORDER BY fetch_ts DESC
            LIMIT 1
            """,
            (event_date.isoformat(),),
        ).fetchone()
        prev_high = float(prev[0]) if prev and prev[0] is not None else None
        changed = (prev_high is not None) and (float(running_high_f) != prev_high)
        conn.execute(
            """
            INSERT INTO dsm_observations (
                fetch_ts, issuance_ts, event_date, running_high_f, raw_text
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                fetch_s,
                issuance_s,
                event_date.isoformat(),
                float(running_high_f),
                str(raw_text),
            ),
        )
        conn.commit()
        return changed
    finally:
        conn.close()


def log_cli_observation(
    db_path: Path,
    fetch_ts_utc: datetime,
    issuance_ts_utc: Optional[datetime],
    event_date: date,
    cli_high_f: float,
    is_preliminary: bool,
    raw_text: str,
) -> bool:
    """
    Insert one CLI observation row.
    Returns True when this is the first detected CLI high for event_date.
    """
    if fetch_ts_utc.tzinfo is None:
        fetch_ts_utc = fetch_ts_utc.replace(tzinfo=timezone.utc)
    fetch_s = _utc_z(fetch_ts_utc)
    issuance_s = _utc_z(issuance_ts_utc) if issuance_ts_utc is not None else None

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        ensure_schema(conn)
        prev_cnt = conn.execute(
            """
            SELECT COUNT(*)
            FROM cli_observations
            WHERE event_date = ?
            """,
            (event_date.isoformat(),),
        ).fetchone()
        first_detected = int(prev_cnt[0]) == 0 if prev_cnt else True
        conn.execute(
            """
            INSERT INTO cli_observations (
                fetch_ts, issuance_ts, event_date, cli_high_f, is_preliminary, raw_text
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                fetch_s,
                issuance_s,
                event_date.isoformat(),
                float(cli_high_f),
                1 if is_preliminary else 0,
                str(raw_text),
            ),
        )
        conn.commit()
        return first_detected
    finally:
        conn.close()


def latest_cli_observed_high(
    db_path: Path,
    event_date: date,
    final_only: bool = True,
) -> Optional[float]:
    """Return latest CLI high for event_date from cli_observations, if any."""
    conn = sqlite3.connect(str(db_path))
    try:
        ensure_schema(conn)
        if final_only:
            row = conn.execute(
                """
                SELECT cli_high_f
                FROM cli_observations
                WHERE event_date = ?
                  AND is_preliminary = 0
                ORDER BY COALESCE(issuance_ts, fetch_ts) DESC, fetch_ts DESC
                LIMIT 1
                """,
                (event_date.isoformat(),),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT cli_high_f
                FROM cli_observations
                WHERE event_date = ?
                ORDER BY COALESCE(issuance_ts, fetch_ts) DESC, fetch_ts DESC
                LIMIT 1
                """,
                (event_date.isoformat(),),
            ).fetchone()
        return float(row[0]) if row and row[0] is not None else None
    finally:
        conn.close()


def latest_metar_observation_ts_utc(
    db_path: Path,
    day_utc: date,
    station: str = "KNYC",
) -> Optional[datetime]:
    """Return latest observation_ts for station on day_utc (UTC date)."""
    conn = sqlite3.connect(str(db_path))
    try:
        ensure_schema(conn)
        row = conn.execute(
            """
            SELECT MAX(observation_ts)
            FROM metar_observations
            WHERE station = ?
              AND date(observation_ts) = ?
            """,
            (station, day_utc.isoformat()),
        ).fetchone()
        if not row or not row[0]:
            return None
        ts = datetime.fromisoformat(str(row[0]).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)
    finally:
        conn.close()


def latest_metar_wind_speed_kt(
    db_path: Path,
    as_of_utc: datetime,
    station: str = "KNYC",
) -> Optional[int]:
    """Most recent non-null wind_speed_kt in metar_observations at or before as_of_utc (METAR; not NWS grid)."""
    if as_of_utc.tzinfo is None:
        as_of_utc = as_of_utc.replace(tzinfo=timezone.utc)
    as_of_s = _utc_z(as_of_utc)
    conn = sqlite3.connect(str(db_path))
    try:
        ensure_schema(conn)
        row = conn.execute(
            """
            SELECT wind_speed_kt
            FROM metar_observations
            WHERE station = ?
              AND observation_ts <= ?
              AND wind_speed_kt IS NOT NULL
            ORDER BY observation_ts DESC
            LIMIT 1
            """,
            (station, as_of_s),
        ).fetchone()
        if not row or row[0] is None:
            return None
        return int(row[0])
    finally:
        conn.close()


def latest_morning_overnight_and_record_high(
    db_path: Path,
    event_date: date,
) -> Tuple[Optional[float], Optional[float]]:
    """overnight_low_f and record_high_f from the latest morning snapshot for event_date (any bracket row)."""
    conn = sqlite3.connect(str(db_path))
    try:
        ensure_schema(conn)
        row = conn.execute(
            """
            SELECT overnight_low_f, record_high_f
            FROM bracket_snapshots
            WHERE event_date = ? AND snapshot_type = 'morning'
            ORDER BY snapshot_ts DESC
            LIMIT 1
            """,
            (event_date.isoformat(),),
        ).fetchone()
        if not row:
            return None, None
        ol, rh = row[0], row[1]
        return (
            float(ol) if ol is not None else None,
            float(rh) if rh is not None else None,
        )
    finally:
        conn.close()


def log_metar_observations(
    db_path: Path,
    observations: List[Dict[str, Any]],
    fetch_ts_utc: datetime,
    station: str = "KNYC",
) -> int:
    """
    Insert/ignore METAR observations.
    observations items expect keys:
      observation_ts (datetime), tmpf (Optional[float]), wind_dir_deg (Optional[int]),
      wind_speed_kt (Optional[int]), sky_cover (Optional[str])
    Returns number of rows inserted.
    """
    if fetch_ts_utc.tzinfo is None:
        fetch_ts_utc = fetch_ts_utc.replace(tzinfo=timezone.utc)
    fetch_s = _utc_z(fetch_ts_utc)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        ensure_schema(conn)
        before = conn.total_changes
        for ob in observations:
            obs_ts = ob.get("observation_ts")
            if not isinstance(obs_ts, datetime):
                continue
            if obs_ts.tzinfo is None:
                obs_ts = obs_ts.replace(tzinfo=timezone.utc)
            obs_s = _utc_z(obs_ts)
            conn.execute(
                """
                INSERT OR IGNORE INTO metar_observations (
                    observation_ts, station, tmpf, wind_dir_deg, wind_speed_kt, sky_cover, fetch_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    obs_s,
                    station,
                    ob.get("tmpf"),
                    ob.get("wind_dir_deg"),
                    ob.get("wind_speed_kt"),
                    ob.get("sky_cover"),
                    fetch_s,
                ),
            )
        conn.commit()
        return int(conn.total_changes - before)
    finally:
        conn.close()


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
    """Approx hours between snapshot_ts and expected final CLI release (~03:00 local the following morning; PROVISIONAL)."""
    z = ZoneInfo(tz_name)
    settle_local = datetime(event_date.year, event_date.month, event_date.day + 1, 3, 0, tzinfo=z)
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

    # wind_speed_kt: morning rows use NWS grid 7am (wind_speed_kt_7am); intraday uses METAR
    # (wind_speed_kt). Different instruments/physics — fine for v0 logging; do not blend naïvely in analysis.
    ws_raw = nws_log_context.get("wind_speed_kt_7am")
    if ws_raw is None:
        ws_raw = nws_log_context.get("wind_speed_kt")
    wind_speed_kt = int(round(float(ws_raw))) if ws_raw is not None else None

    # overnight_low_f: morning from NWS grid minTemperature (wide time window; see morning_model stderr);
    # intraday copied from latest morning snapshot for that event_date.
    ol_raw = nws_log_context.get("overnight_low_f")
    overnight_low_f = float(ol_raw) if ol_raw is not None else None

    if snapshot_type == "intraday":
        rh_raw = nws_log_context.get("record_high_f")
        record_high_f = float(rh_raw) if rh_raw is not None else None
    else:
        # record_high_f requires records.json (365 KNYC calendar-day record highs). Repo does not
        # include that file yet — column stays NULL until it is built and path is provided.
        record_high_f = _record_high_for_date(event_date, records_path)

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
                    wind_speed_kt,
                    sky_cover,
                    overnight_low_f,
                    record_high_f,
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
    - actual_max_f on every row with this event_date (from CLI / fallback fetchers)
    - outcome = 1 for the bracket that wins under Kalshi NHIGH rules, else 0

    Settlement is computed from bracket_label first (CFTC: strict tails, inclusive
    integer bands), so it stays correct even if legacy rows have uncorrected
    bracket_lower_f/bracket_upper_f. If the label is unparsable, falls back to
    half-open checks on stored bounds (low tail: actual < upper; high tail:
    actual > lower; middle: lower <= actual < upper).

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
            label_s = str(label)
            try:
                is_win = kalshi_settlement_wins(label_s, float(actual_max_f))
            except ValueError:
                lo_f = float(lo) if lo is not None else None
                hi_f = float(hi) if hi is not None else None
                if lo_f is None and hi_f is None:
                    is_win = False
                elif lo_f is None:
                    is_win = actual_max_f < hi_f  # type: ignore[operator]
                elif hi_f is None:
                    is_win = actual_max_f > lo_f
                else:
                    is_win = (lo_f <= actual_max_f) and (actual_max_f < hi_f)
            outcomes.append((label_s, 1 if is_win else 0))

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
    Fetch the daily maximum temperature (°F) for KNYC from the IEM
    Daily Climate Report archive:
    https://mesonet.agron.iastate.edu/cgi-bin/request/daily.py

    ASOS hourly max is not the official NWS CLI high and is never used here.

    Raises RuntimeError if the report is unavailable or returns no usable max.
    Wait and re-run backfill, or pass --actual-max with the official high.
    """
    import csv
    import io
    from urllib.parse import urlencode
    from urllib.request import Request, urlopen

    DAILY_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/daily.py"
    station = "KNYC"
    network = "NY_ASOS"
    daily_error: Optional[str] = None
    rows: List[Dict[str, str]] = []
    daily_keys: List[str] = []
    try:
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
        with urlopen(req, timeout=60) as resp:
            text = resp.read().decode("utf-8", errors="replace")
        buf = io.StringIO(text)
        reader = csv.DictReader(buf)
        rows = list(reader)
        daily_keys = list(rows[0].keys()) if rows else []
    except Exception as e:
        daily_error = str(e)

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
        key = _pick_max_key(daily_keys)
        if key is not None:
            raw = (rows[0].get(key) or "").strip()
            if raw not in ("", "M", "NA", "None", "null"):
                try:
                    return float(raw)
                except ValueError:
                    pass

    if target_date >= date.today():
        if daily_error:
            raise RuntimeError(
                f"No daily climate report data available yet for {target_date} ({daily_error}). "
                "Wait and re-run later, or use --actual-max."
            )
        raise RuntimeError(
            f"No daily climate report data available yet for {target_date}. "
            "Wait and re-run later, or use --actual-max."
        )
    if daily_error:
        raise RuntimeError(
            f"IEM daily climate report request failed for KNYC on {target_date}: {daily_error}. "
            "Wait and re-run later, or use --actual-max with the official NWS high."
        )
    raise RuntimeError(
        f"No usable daily climate report data for KNYC on {target_date} "
        "(empty response or missing max field). "
        "Wait and re-run later, or use --actual-max with the official NWS high."
    )


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
        print(f"Fetching KNYC daily max for {target} from IEM daily climate report...")
        try:
            actual = fetch_knyc_cli_max(target)
        except RuntimeError as e:
            print(str(e), file=sys.stderr)
            raise SystemExit(2)
        print(f"IEM daily report actual_max_f: {actual}")

    n = backfill_outcome(args.db, target, actual)
    print(f"Updated {n} rows for event_date={target}")
