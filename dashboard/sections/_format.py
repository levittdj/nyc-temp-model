"""Shared display helpers (cents formatting, UTC -> ET)."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

ET = ZoneInfo("America/New_York")


def cents(dollars: Optional[float]) -> str:
    if dollars is None or (isinstance(dollars, float) and pd.isna(dollars)):
        return ""
    return f"{float(dollars) * 100:.1f}\u00a2"


def to_et(ts: object) -> str:
    if ts is None or (isinstance(ts, float) and pd.isna(ts)) or ts == "":
        return ""
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return str(ts)
    return dt.astimezone(ET).strftime("%Y-%m-%d %H:%M")
