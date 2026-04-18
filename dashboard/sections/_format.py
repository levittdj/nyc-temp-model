"""Shared display helpers (dollar formatting, UTC -> ET)."""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from typing import Optional

import pandas as pd

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

ET = ZoneInfo("America/New_York")


def dollars(value: Optional[float]) -> str:
    """Format a Kalshi-scale dollar amount (contract settles $0..$1) as $X.XX.

    Works for both per-contract prices (0-1 fractions) and P&L sums (can be larger).
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    v = float(value)
    if v < 0:
        return f"-${abs(v):.2f}"
    return f"${v:.2f}"


def to_et(ts: object) -> str:
    """Format a UTC ISO-8601 timestamp string as `YYYY-MM-DD HH:MM` in ET."""
    if ts is None or (isinstance(ts, float) and pd.isna(ts)) or ts == "":
        return ""
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return str(ts)
    return dt.astimezone(ET).strftime("%Y-%m-%d %H:%M")


def hours_to_settle_live(event_date_val: object,
                         now_utc: Optional[datetime] = None) -> Optional[float]:
    """Live hours between ``now`` and Kalshi NHIGH settlement.

    Settlement is ~03:00 ET on the day after ``event_date`` (PROVISIONAL; same
    definition as ``logger._hours_to_settle``).  Computed live at page-load
    so open-position rows don't get stuck on the value logged at the latest
    bracket_snapshots row (which can be several hours stale between ticks).

    Returns None if ``event_date_val`` can't be parsed.
    """
    if (event_date_val is None
            or (isinstance(event_date_val, float) and pd.isna(event_date_val))
            or event_date_val == ""):
        return None
    try:
        ed = datetime.strptime(str(event_date_val)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None
    settle_local = datetime.combine(ed + timedelta(days=1), time(3, 0), tzinfo=ET)
    now = now_utc if now_utc is not None else datetime.now(timezone.utc)
    delta = settle_local - now.astimezone(ET)
    return delta.total_seconds() / 3600.0
