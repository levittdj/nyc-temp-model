"""Blocked-signal panel: counts by reason + recent-20 list."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from queries import blocked_signals_by_reason, blocked_signals_recent

from ._format import to_et

_USD = st.column_config.NumberColumn(format="$%.2f")


def render_blocked_signals(conn, start_utc: str, end_utc: str) -> None:
    st.subheader("Blocked signals")
    by_reason = blocked_signals_by_reason(conn, start_utc, end_utc)
    recent = blocked_signals_recent(conn, start_utc, end_utc, limit=20)

    if by_reason.empty and recent.empty:
        st.info("No blocked intraday_signals in this range.")
        return

    c1, c2 = st.columns([1, 2])
    with c1:
        st.markdown("**Counts by reason**")
        if by_reason.empty:
            st.info("No blocked signals.")
        else:
            st.dataframe(by_reason, use_container_width=True, hide_index=True)
    with c2:
        st.markdown("**Most recent 20 blocked signals**")
        if recent.empty:
            st.info("No blocked signals in range.")
            return
        r = recent.copy()
        r["snapshot (ET)"] = r["snapshot_ts"].map(to_et)
        r["edge %"] = (pd.to_numeric(r["edge"], errors="coerce") * 100).round(1)
        r["model %"] = (pd.to_numeric(r["model_prob"], errors="coerce") * 100).round(1)
        r["market $"] = pd.to_numeric(r["market_price"], errors="coerce")
        cols = ["snapshot (ET)", "bracket_label", "signal_type", "reason",
                "edge %", "model %", "market $"]
        st.dataframe(
            r[cols].rename(columns={"bracket_label": "bracket"}),
            use_container_width=True, hide_index=True,
            column_config={"market $": _USD},
        )
    st.caption(
        "No counterfactual P&L here \u2014 block-reasoning only.  "
        "`reason` strings come straight from intraday_engine.py signal gating."
    )
