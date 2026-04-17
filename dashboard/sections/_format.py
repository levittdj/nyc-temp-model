"""Shared display helpers (dollar formatting, UTC -> ET)."""

from __future__ import annotations

from datetime import datetime
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
