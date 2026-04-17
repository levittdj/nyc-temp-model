"""
NYC Temp Paper Trading dashboard — v0 review only.  Read-only DB access.

No live execution.  No caching (yet).  Every page load runs fresh queries.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

from db import get_ro_connection
from filters import render_timeframe_filter
from queries import (
    closed_positions_in_range,
    cumulative_net_pnl_all_time,
    latest_snapshot_ts,
    trades_in_range,
)

ET = ZoneInfo("America/New_York")

_DEFAULT_DB = Path(__file__).resolve().parent.parent / "nyc_temp_log.sqlite"
DB_PATH = Path(os.environ.get("NYC_TEMP_DB", str(_DEFAULT_DB)))

st.set_page_config(page_title="NYC Temp Paper Trading", layout="wide")
st.title("NYC Temp Paper Trading \u2014 v0 review")

if not DB_PATH.exists():
    st.error(f"Database not found: {DB_PATH}")
    st.stop()


def _cents(dollars: float) -> str:
    return f"{dollars * 100:.1f}\u00a2"


def _to_et(ts: object) -> str:
    if ts is None or (isinstance(ts, float) and pd.isna(ts)) or ts == "":
        return ""
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return str(ts)
    return dt.astimezone(ET).strftime("%Y-%m-%d %H:%M")


conn = get_ro_connection(DB_PATH)
snap_ts = latest_snapshot_ts(conn) or "\u2014"
st.caption(f"Latest snapshot: {snap_ts}  \u00b7  DB: `{DB_PATH.name}`")

tf = render_timeframe_filter()
start_utc, end_utc = tf["start_utc"], tf["end_utc"]

closed = closed_positions_in_range(conn, start_utc, end_utc)
trades = trades_in_range(conn, start_utc, end_utc)
cum_all = cumulative_net_pnl_all_time(conn)
conn.close()

n_closed = len(closed)
pnl_net_sum = float(closed["pnl_net"].fillna(0).sum()) if n_closed else 0.0
pnl_gross_sum = float(closed["pnl_gross"].fillna(0).sum()) if n_closed else 0.0
fee_sum = (
    float((closed["entry_fee"].fillna(0) + closed["exit_fee"].fillna(0)).sum())
    if n_closed
    else 0.0
)
win_rate = float((closed["pnl_net"] > 0).mean()) if n_closed else 0.0
fee_drag_pct = fee_sum / max(abs(pnl_gross_sum), 0.01) * 100.0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric(f"Period Net P&L ({tf['label']})", _cents(pnl_net_sum))
k2.metric("Win rate", f"{win_rate * 100:.1f}%")
k3.metric("Trades closed", n_closed)
k4.metric("Fee drag", f"{fee_drag_pct:.1f}%")
k5.metric("All-time Net P&L", _cents(cum_all))

st.subheader("Equity curve")
if n_closed:
    curve = closed.copy()
    curve["anchor"] = curve["exit_ts"].where(
        curve["exit_ts"].notna() & (curve["exit_ts"] != ""), curve["entry_ts"]
    )
    curve["anchor_dt"] = pd.to_datetime(curve["anchor"], utc=True, errors="coerce")
    curve = curve.sort_values("anchor_dt").reset_index(drop=True)
    curve["cum_net_cents"] = curve["pnl_net"].fillna(0).cumsum() * 100.0
    st.line_chart(
        curve.set_index("anchor_dt")[["cum_net_cents"]], use_container_width=True
    )
else:
    st.info("No closed trades in this range.")

st.subheader("Trade log")
if len(trades):
    t = trades.copy()
    t["entry (ET)"] = t["entry_ts"].map(_to_et)
    t["entry \u00a2"] = (t["avg_entry_price"].astype(float) * 100).round(1)
    t["exit \u00a2"] = (t["exit_price"].astype(float) * 100).round(1)
    t["gross \u00a2"] = (t["pnl_gross"].fillna(0).astype(float) * 100).round(1)
    t["net \u00a2"] = (t["pnl_net"].fillna(0).astype(float) * 100).round(1)
    show = t[
        [
            "event_date",
            "bracket_label",
            "side",
            "contracts",
            "entry (ET)",
            "entry \u00a2",
            "exit \u00a2",
            "settlement_outcome",
            "status",
            "gross \u00a2",
            "net \u00a2",
            "signal_reason",
        ]
    ].rename(
        columns={
            "bracket_label": "bracket",
            "settlement_outcome": "settled",
            "signal_reason": "reason",
        }
    )
    st.dataframe(show, use_container_width=True, hide_index=True)
else:
    st.info("No trades in this range.")
