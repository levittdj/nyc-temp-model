"""Signal decomposition, signal-type breakdown, fee detail."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from queries import fee_details, signal_decomposition, signal_type_breakdown

from ._format import cents


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
