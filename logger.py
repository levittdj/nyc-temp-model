#!/usr/bin/env python3
"""
v0 SQLite log: one row per bracket-day with 8 bracket-level + 12 day-level fields.
Invoked from morning_model after each run. Terminal review on stderr (stdout stays JSON).
"""

from __future__ import annotations

import math
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional


DEFAULT_DB_NAME = "nyc_temp_log.sqlite"

SCHEMA = """
CREATE TABLE IF NOT EXISTS bracket_days (
    date TEXT NOT NULL,
    bracket_label TEXT NOT NULL,
    bracket_lower_f REAL,
    bracket_upper_f REAL,
    model_prob REAL NOT NULL,
    market_price REAL NOT NULL,
    edge REAL NOT NULL,
    outcome INTEGER,
    actual_max_f REAL,
    nbm_p10 REAL NOT NULL,
    nbm_p50 REAL NOT NULL,
    nbm_p90 REAL NOT NULL,
    nbm_bias_applied REAL NOT NULL,
    nbm_spread REAL NOT NULL,
    wind_dir TEXT,
    sky_cover TEXT,
    overnight_low_f REAL,
    record_high_f REAL,
    record_prox_flag INTEGER NOT NULL,
    pull_timestamp TEXT NOT NULL,
    PRIMARY KEY (date, bracket_label)
);
"""


def _bounds_db(lower_f: float, upper_f: float) -> tuple[Optional[float], Optional[float]]:
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


def _wind_sky_from_nws(ctx: dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
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


def log_morning_run(
    db_path: Path,
    target: date,
    rows: list[Any],
    pct_f_raw: tuple[float, float, float],
    nbm_bias: float,
    record_prox_flag: bool,
    nws_log_context: dict[str, Any],
    pull_timestamp_utc: datetime,
    records_path: Optional[Path] = None,
) -> None:
    """Insert or replace all bracket rows for target date (full snapshot per run)."""
    p10, p50, p90 = pct_f_raw
    nbm_spread = p90 - p10
    wind_dir, sky_cover = _wind_sky_from_nws(nws_log_context)
    rh = _record_high_for_date(target, records_path)
    ts = pull_timestamp_utc
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    pull_s = ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        ensure_schema(conn)
        conn.execute("DELETE FROM bracket_days WHERE date = ?", (target.isoformat(),))
        for r in rows:
            label = bracket_label(r.lower_f, r.upper_f)
            lo_db, hi_db = _bounds_db(r.lower_f, r.upper_f)
            conn.execute(
                """
                INSERT INTO bracket_days (
                    date, bracket_label, bracket_lower_f, bracket_upper_f,
                    model_prob, market_price, edge, outcome,
                    actual_max_f,
                    nbm_p10, nbm_p50, nbm_p90, nbm_bias_applied, nbm_spread,
                    wind_dir, sky_cover, overnight_low_f, record_high_f,
                    record_prox_flag, pull_timestamp
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    target.isoformat(),
                    label,
                    lo_db,
                    hi_db,
                    r.model_prob,
                    r.market_price,
                    r.edge,
                    None,
                    None,
                    p10,
                    p50,
                    p90,
                    nbm_bias,
                    nbm_spread,
                    wind_dir,
                    sky_cover,
                    None,
                    rh,
                    1 if record_prox_flag else 0,
                    pull_s,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def print_terminal_review(target: date, rows: list[Any], file: Any = None) -> None:
    """Human-readable bracket table; outcome column reserved for backfill (shown as —)."""
    import sys

    out = file if file is not None else sys.stderr
    lines = [
        f"[logger] {target.isoformat()}  model_prob vs market_price vs outcome (outcome backfilled later)",
        f"{'label':<12} {'p_model':>8} {'p_mkt':>8} {'edge':>8} {'outcome':>8}",
        "-" * 52,
    ]
    for r in rows:
        lab = bracket_label(r.lower_f, r.upper_f)
        lines.append(
            f"{lab:<12} {r.model_prob:8.4f} {r.market_price:8.4f} {r.edge:8.4f} {'—':>8}"
        )
    print("\n".join(lines), file=out)
