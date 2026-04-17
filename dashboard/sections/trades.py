"""Equity curve, trade log, open positions."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from queries import open_positions_with_unrealized

from ._format import ET, dollars, to_et

_USD = st.column_config.NumberColumn(format="$%.2f")


def render_equity_curve(closed: pd.DataFrame) -> None:
    st.subheader("Equity curve")
    if closed.empty:
        st.info("No closed trades in this range.")
        return
    curve = closed.copy()
    curve["anchor"] = curve["exit_ts"].where(
        curve["exit_ts"].notna() & (curve["exit_ts"] != ""), curve["entry_ts"]
    )
    curve["anchor_dt"] = (
        pd.to_datetime(curve["anchor"], utc=True, errors="coerce")
        .dt.tz_convert(ET)
    )
    curve = curve.sort_values("anchor_dt").reset_index(drop=True)
    curve["Cumulative Net P&L ($)"] = curve["pnl_net"].fillna(0).cumsum()
    st.line_chart(
        curve.set_index("anchor_dt")[["Cumulative Net P&L ($)"]],
        use_container_width=True,
        x_label="Time (ET)",
        y_label="Net P&L ($)",
    )


def render_trade_log(trades: pd.DataFrame) -> None:
    st.subheader("Trade log")
    if trades.empty:
        st.info("No trades in this range.")
        return
    t = trades.copy()
    t["entry (ET)"] = t["entry_ts"].map(to_et)
    t["entry $"] = t["avg_entry_price"].astype(float)
    t["exit $"] = pd.to_numeric(t["exit_price"], errors="coerce")
    t["gross $"] = t["pnl_gross"].fillna(0).astype(float)
    t["net $"] = t["pnl_net"].fillna(0).astype(float)
    cols = [
        "event_date", "bracket_label", "side", "contracts",
        "entry (ET)", "entry $", "exit $",
        "settlement_outcome", "status", "gross $", "net $",
        "signal_reason",
    ]
    show = t[cols].rename(columns={
        "bracket_label": "bracket",
        "settlement_outcome": "settled",
        "signal_reason": "reason",
    })
    st.dataframe(
        show,
        use_container_width=True,
        hide_index=True,
        column_config={
            "entry $": _USD, "exit $": _USD, "gross $": _USD, "net $": _USD,
        },
    )


def render_open_positions(conn) -> None:
    st.subheader("Open positions")
    op = open_positions_with_unrealized(conn)
    if op.empty:
        st.info("No open positions.")
        return
    total_unrealized = float(op["unrealized_pnl"].fillna(0).sum())
    st.metric("Total unrealized P&L", dollars(total_unrealized))
    display = pd.DataFrame({
        "event_date": op["event_date"],
        "bracket": op["bracket_label"],
        "side": op["side"],
        "contracts": op["contracts"],
        "entry $": op["avg_entry_price"].astype(float),
        "current $": pd.to_numeric(op["current_mkt"], errors="coerce"),
        "unrealized $": op["unrealized_pnl"].astype(float),
        "hrs to settle": pd.to_numeric(op["hours_to_settle"], errors="coerce").round(1),
        "reason": op["signal_reason"],
    })
    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "entry $": _USD, "current $": _USD, "unrealized $": _USD,
        },
    )
