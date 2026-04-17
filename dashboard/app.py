"""
NYC Temp Paper Trading dashboard — v0 review only.  Read-only DB access.

No live execution.  No caching (yet).  Every page load runs fresh queries.
Section rendering lives in dashboard/sections.py; all SQL in queries.py.
"""

from __future__ import annotations

import os
from pathlib import Path

import streamlit as st

from db import get_ro_connection
from filters import render_timeframe_filter
from queries import (
    closed_positions_in_range,
    cumulative_net_pnl_all_time,
    latest_snapshot_ts,
    trades_in_range,
)
from sections import (
    dollars,
    # render_blocked_signals,
    # render_calibration,
    render_equity_curve,
    render_fee_detail,
    # render_nbm_revisions,
    render_open_positions,
    render_signal_decomposition,
    render_signal_type_breakdown,
    render_trade_log,
)
from sections._format import to_et
from style import inject_global_css

_DEFAULT_DB = Path(__file__).resolve().parent.parent / "nyc_temp_log.sqlite"
DB_PATH = Path(os.environ.get("NYC_TEMP_DB", str(_DEFAULT_DB)))

st.set_page_config(page_title="NYC Temp Paper Trading", layout="wide")
inject_global_css()
st.title("NYC Temp Paper Trading \u2014 v0 review")

if not DB_PATH.exists():
    st.error(f"Database not found: {DB_PATH}")
    st.stop()

conn = get_ro_connection(DB_PATH)
_raw_ts = latest_snapshot_ts(conn)
snap_ts = f"{to_et(_raw_ts)} ET" if _raw_ts else "\u2014"
st.caption(f"Latest snapshot: {snap_ts}  \u00b7  DB: `{DB_PATH.name}`")

tf = render_timeframe_filter()
start_utc, end_utc = tf["start_utc"], tf["end_utc"]

closed = closed_positions_in_range(conn, start_utc, end_utc)
trades = trades_in_range(conn, start_utc, end_utc)
cum_all = cumulative_net_pnl_all_time(conn)

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
k1.metric(f"Period Net P&L ({tf['label']})", dollars(pnl_net_sum))
k2.metric("Win rate", f"{win_rate * 100:.1f}%")
k3.metric("Trades closed", n_closed)
k4.metric("Fee drag", f"{fee_drag_pct:.1f}%")
k5.metric("All-time Net P&L", dollars(cum_all))

render_equity_curve(closed)
render_trade_log(trades)
render_open_positions(conn)
render_signal_decomposition(conn)
render_signal_type_breakdown(conn)
# render_calibration(conn, start_utc, end_utc)
# render_nbm_revisions(conn, start_utc, end_utc)
# render_blocked_signals(conn, start_utc, end_utc)
render_fee_detail(conn)

conn.close()
