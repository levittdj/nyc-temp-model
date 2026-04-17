"""Signal decomposition, signal-type breakdown, fee detail."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from queries import fee_details, signal_decomposition, signal_type_breakdown
from style import section_header

from ._format import dollars

_USD = st.column_config.NumberColumn(format="$%.2f")


def render_signal_decomposition(conn) -> None:
    section_header("SIGNALS", "Signal decomposition")
    df = signal_decomposition(conn)
    if df.empty:
        st.info("No closed positions with signal-source joins yet.")
        return
    chart_df = df.set_index("signal_source")[["total_pnl"]].copy()
    chart_df = chart_df.rename(columns={"total_pnl": "Total Net P&L ($)"})
    c1, c2 = st.columns([1, 1])
    with c1:
        st.bar_chart(chart_df, horizontal=True, use_container_width=True,
                     x_label="Total Net P&L ($)")
    with c2:
        tbl = pd.DataFrame({
            "source": df["signal_source"],
            "n": df["n"],
            "wins": df["wins"],
            "losses": df["losses"],
            "mean $": df["mean_pnl"].astype(float),
            "total $": df["total_pnl"].astype(float),
        })
        st.dataframe(
            tbl, use_container_width=True, hide_index=True,
            column_config={"mean $": _USD, "total $": _USD},
        )


def render_signal_type_breakdown(conn) -> None:
    section_header("SIGNALS", "Signal type breakdown")
    df = signal_type_breakdown(conn)
    if df.empty:
        st.info("No closed positions with signal_type yet.")
        return
    tbl = pd.DataFrame({
        "signal_type": df["signal_type"],
        "n": df["n"],
        "wins": df["wins"],
        "losses": df["losses"],
        "avg winner $": df["avg_winner"].astype(float),
        "avg loser $": df["avg_loser"].astype(float),
        "mean edge @ entry %": (df["mean_edge_at_entry"].astype(float) * 100).round(2),
    })
    st.dataframe(
        tbl, use_container_width=True, hide_index=True,
        column_config={"avg winner $": _USD, "avg loser $": _USD},
    )


def render_fee_detail(conn) -> None:
    fd = fee_details(conn)
    with st.expander("Fee detail (sanity check vs intraday_engine.estimate_fee)"):
        a, b, c, d = st.columns(4)
        a.metric("Total entry fees", dollars(fd["total_entry_fees"]))
        b.metric("Total exit fees", dollars(fd["total_exit_fees"]))
        c.metric("Fee drag", f"{fd['fee_drag_pct']:.1f}%")
        d.metric(
            "Avg fee per contract",
            dollars(fd["avg_fee_per_contract"]) if fd["total_contracts"] else "\u2014",
        )
        st.caption(
            f"Based on {fd['total_contracts']} contracts across closed positions; "
            f"gross P&L = {dollars(fd['total_gross'])}"
        )
