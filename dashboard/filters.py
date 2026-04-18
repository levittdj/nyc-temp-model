"""
Time-frame filter widget.

Returns a dict with UTC ISO-8601 Z strings (`start_utc`, `end_utc`) plus the
ET calendar dates (`start_date`, `end_date`) and the chosen `label`.  `end_utc`
is the EXCLUSIVE midnight-ET of the day AFTER `end_date`, so SQL filters use
`entry_ts >= start_utc AND entry_ts < end_utc`.

Boundaries are computed in America/New_York so "Today" means today ET even
though entry_ts is stored as UTC.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Dict

import streamlit as st

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

ET = ZoneInfo("America/New_York")

_OPTIONS = ["Today", "Last 7 days", "Last 30 days", "All time", "Custom"]
_DEFAULT = "Today"


def _et_midnight_utc(d: date) -> str:
    dt = datetime(d.year, d.month, d.day, tzinfo=ET).astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def render_timeframe_filter() -> Dict[str, object]:
    """Render the segmented control and return the resolved range."""
    today_et = datetime.now(ET).date()
    choice = st.segmented_control(
        "Time frame", options=_OPTIONS, default=_DEFAULT, key="timeframe"
    )
    if choice is None:
        choice = _DEFAULT

    if choice == "Today":
        start, end = today_et, today_et
    elif choice == "Last 7 days":
        start, end = today_et - timedelta(days=6), today_et
    elif choice == "Last 30 days":
        start, end = today_et - timedelta(days=29), today_et
    elif choice == "All time":
        start, end = date(2000, 1, 1), today_et
    else:  # Custom
        c1, c2 = st.columns(2)
        start = c1.date_input(
            "Start", value=today_et - timedelta(days=6), key="tf_start"
        )
        end = c2.date_input("End", value=today_et, key="tf_end")
        if end < start:
            st.warning("End date is before start; swapping.")
            start, end = end, start

    return {
        "label": choice,
        "start_date": start,
        "end_date": end,
        "start_utc": _et_midnight_utc(start),
        "end_utc": _et_midnight_utc(end + timedelta(days=1)),
    }
