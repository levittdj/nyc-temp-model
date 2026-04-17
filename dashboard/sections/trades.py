"""Equity curve, trade log, open positions."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from queries import open_positions_with_unrealized

from ._format import cents, to_et


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
