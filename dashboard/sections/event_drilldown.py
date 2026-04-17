"""Per-event drill-down page content (header, price chart, signals, positions)."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from queries import (
    bracket_price_trajectory,
    dsm_running_high,
    event_day_positions,
    event_day_signals,
    event_day_summary,
)

from ._format import cents, to_et


def _render_header(conn, event_date: str) -> None:
    s = event_day_summary(conn, event_date)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Actual max (F)",
              f"{s['actual_max_f']:.1f}" if s["actual_max_f"] is not None else "\u2014")
    c2.metric("Winning bracket", s["winning_bracket"] or "\u2014")
    if s["morning_p50"] is not None and s["final_p50"] is not None:
        delta = s["final_p50"] - s["morning_p50"]
        c3.metric("NBM p50: morning \u2192 final",
                  f"{s['morning_p50']:.1f} \u2192 {s['final_p50']:.1f}F",
                  delta=f"{delta:+.1f}F")
    else:
        c3.metric("NBM p50: morning \u2192 final", "\u2014")
    c4.metric("Day Net P&L", cents(s["pnl_net"]))


def _render_price_chart(conn, event_date: str) -> None:
    traj = bracket_price_trajectory(conn, event_date)
    if traj.empty:
        st.info("No intraday bracket snapshots for this event_date.")
        return
    dsm = dsm_running_high(conn, event_date)

    try:
        import plotly.graph_objects as go
        from plotly.colors import qualitative
    except ImportError as exc:
        st.warning(f"Plotly unavailable ({exc}); cannot render price chart.")
        return

    palette = qualitative.Plotly
    brackets = sorted(traj["bracket_label"].unique())
    traj = traj.copy()
    traj["snapshot_dt"] = pd.to_datetime(traj["snapshot_ts"], utc=True, errors="coerce")

    fig = go.Figure()
    for i, lbl in enumerate(brackets):
        color = palette[i % len(palette)]
        sub = traj[traj["bracket_label"] == lbl].sort_values("snapshot_dt")
        fig.add_trace(go.Scatter(
            x=sub["snapshot_dt"], y=sub["market_price"] * 100,
            mode="lines", name=f"{lbl} mkt", line=dict(color=color, width=2),
            legendgroup=lbl,
        ))
        fig.add_trace(go.Scatter(
            x=sub["snapshot_dt"], y=sub["model_prob"] * 100,
            mode="lines", name=f"{lbl} model",
            line=dict(color=color, width=1.5, dash="dash"),
            legendgroup=lbl, showlegend=True,
        ))

    if not dsm.empty:
        dsm = dsm.copy()
        dsm["fetch_dt"] = pd.to_datetime(dsm["fetch_ts"], utc=True, errors="coerce")
        fig.add_trace(go.Scatter(
            x=dsm["fetch_dt"], y=dsm["running_high_f"],
            mode="lines+markers", name="DSM running high (F)",
            line=dict(color="black", width=2), yaxis="y2",
        ))

    fig.update_layout(
        height=520,
        margin=dict(l=10, r=10, t=30, b=10),
        xaxis_title="snapshot_ts (UTC)",
        yaxis=dict(title="price / model prob (\u00a2 or %)", range=[0, 100]),
        yaxis2=dict(title="running high (F)", overlaying="y", side="right"),
        legend=dict(orientation="h", y=-0.25),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_signals_table(conn, event_date: str) -> None:
    st.markdown("**Signals timeline (executed + blocked)**")
    df = event_day_signals(conn, event_date)
    if df.empty:
        st.info("No intraday_signals for this event_date.")
        return
    r = df.copy()
    r["snapshot (ET)"] = r["snapshot_ts"].map(to_et)
    r["edge %"] = (pd.to_numeric(r["edge"], errors="coerce") * 100).round(1)
    cols = ["snapshot (ET)", "bracket_label", "signal_type", "executed",
            "reason", "contracts_suggested", "edge %"]
    st.dataframe(r[cols].rename(columns={"bracket_label": "bracket"}),
                 use_container_width=True, hide_index=True)


def _render_positions_table(conn, event_date: str) -> None:
    st.markdown("**Paper positions for this event_date**")
    df = event_day_positions(conn, event_date)
    if df.empty:
        st.info("No paper_positions for this event_date.")
        return
    r = df.copy()
    r["entry (ET)"] = r["entry_ts"].map(to_et)
    r["entry \u00a2"] = (r["avg_entry_price"].astype(float) * 100).round(1)
    r["exit \u00a2"] = (pd.to_numeric(r["exit_price"], errors="coerce") * 100).round(1)
    r["net \u00a2"] = (r["pnl_net"].fillna(0).astype(float) * 100).round(1)
    cols = ["bracket_label", "side", "contracts", "entry (ET)",
            "entry \u00a2", "exit \u00a2", "status", "net \u00a2"]
    st.dataframe(r[cols].rename(columns={"bracket_label": "bracket"}),
                 use_container_width=True, hide_index=True)


def render_event_drilldown(conn, event_date: str) -> None:
    st.header(f"Event {event_date}")
    _render_header(conn, event_date)
    st.subheader("Market / model prices with DSM running high")
    _render_price_chart(conn, event_date)
    _render_signals_table(conn, event_date)
    _render_positions_table(conn, event_date)
