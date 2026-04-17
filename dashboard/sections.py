"""
Render helpers for each dashboard section.

Each `render_*` function draws its subheader + widgets and returns None.
Shared formatting helpers live here too so app.py stays a thin orchestrator.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd
import streamlit as st

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

from queries import (
    fee_details,
    open_positions_with_unrealized,
    signal_decomposition,
    signal_type_breakdown,
)

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


def render_equity_curve(closed: pd.DataFrame) -> None:
    st.subheader("Equity curve")
    if closed.empty:
        st.info("No closed trades in this range.")
        return
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


def render_trade_log(trades: pd.DataFrame) -> None:
    st.subheader("Trade log")
    if trades.empty:
        st.info("No trades in this range.")
        return
    t = trades.copy()
    t["entry (ET)"] = t["entry_ts"].map(to_et)
    t["entry \u00a2"] = (t["avg_entry_price"].astype(float) * 100).round(1)
    t["exit \u00a2"] = (t["exit_price"].astype(float) * 100).round(1)
    t["gross \u00a2"] = (t["pnl_gross"].fillna(0).astype(float) * 100).round(1)
    t["net \u00a2"] = (t["pnl_net"].fillna(0).astype(float) * 100).round(1)
    cols = [
        "event_date", "bracket_label", "side", "contracts",
        "entry (ET)", "entry \u00a2", "exit \u00a2",
        "settlement_outcome", "status", "gross \u00a2", "net \u00a2",
        "signal_reason",
    ]
    show = t[cols].rename(columns={
        "bracket_label": "bracket",
        "settlement_outcome": "settled",
        "signal_reason": "reason",
    })
    st.dataframe(show, use_container_width=True, hide_index=True)


def render_open_positions(conn) -> None:
    st.subheader("Open positions")
    op = open_positions_with_unrealized(conn)
    if op.empty:
        st.info("No open positions.")
        return
    total_unrealized = float(op["unrealized_pnl"].fillna(0).sum())
    st.metric("Total unrealized P&L", cents(total_unrealized))
    display = pd.DataFrame({
        "event_date": op["event_date"],
        "bracket": op["bracket_label"],
        "side": op["side"],
        "contracts": op["contracts"],
        "entry \u00a2": (op["avg_entry_price"].astype(float) * 100).round(1),
        "current \u00a2": (pd.to_numeric(op["current_mkt"], errors="coerce") * 100).round(1),
        "unrealized \u00a2": (op["unrealized_pnl"].astype(float) * 100).round(1),
        "hrs to settle": pd.to_numeric(op["hours_to_settle"], errors="coerce").round(1),
        "reason": op["signal_reason"],
    })
    st.dataframe(display, use_container_width=True, hide_index=True)


def render_signal_decomposition(conn) -> None:
    st.subheader("Signal decomposition")
    df = signal_decomposition(conn)
    if df.empty:
        st.info("No closed positions with signal-source joins yet.")
        return
    chart_df = df.set_index("signal_source")[["total_pnl"]].copy()
    chart_df["total_pnl \u00a2"] = chart_df["total_pnl"] * 100.0
    chart_df = chart_df[["total_pnl \u00a2"]]
    c1, c2 = st.columns([1, 1])
    with c1:
        st.bar_chart(chart_df, horizontal=True, use_container_width=True)
    with c2:
        tbl = pd.DataFrame({
            "source": df["signal_source"],
            "n": df["n"],
            "wins": df["wins"],
            "losses": df["losses"],
            "mean \u00a2": (df["mean_pnl"].astype(float) * 100).round(1),
            "total \u00a2": (df["total_pnl"].astype(float) * 100).round(1),
        })
        st.dataframe(tbl, use_container_width=True, hide_index=True)


def render_signal_type_breakdown(conn) -> None:
    st.subheader("Signal type breakdown")
    df = signal_type_breakdown(conn)
    if df.empty:
        st.info("No closed positions with signal_type yet.")
        return
    tbl = pd.DataFrame({
        "signal_type": df["signal_type"],
        "n": df["n"],
        "wins": df["wins"],
        "losses": df["losses"],
        "avg winner \u00a2": (df["avg_winner"].astype(float) * 100).round(1),
        "avg loser \u00a2": (df["avg_loser"].astype(float) * 100).round(1),
        "mean edge @ entry": (df["mean_edge_at_entry"].astype(float) * 100).round(2),
    })
    st.dataframe(tbl, use_container_width=True, hide_index=True)


def render_fee_detail(conn) -> None:
    fd = fee_details(conn)
    with st.expander("Fee detail (sanity check vs intraday_engine.estimate_fee)"):
        a, b, c, d = st.columns(4)
        a.metric("Total entry fees", cents(fd["total_entry_fees"]))
        b.metric("Total exit fees", cents(fd["total_exit_fees"]))
        c.metric("Fee drag", f"{fd['fee_drag_pct']:.1f}%")
        d.metric(
            "Avg fee per contract",
            cents(fd["avg_fee_per_contract"]) if fd["total_contracts"] else "\u2014",
        )
        st.caption(
            f"Based on {fd['total_contracts']} contracts across closed positions; "
            f"gross P&L = {cents(fd['total_gross'])}"
        )
