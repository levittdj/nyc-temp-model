"""Calibration scatter: edge-at-entry vs realised pnl-per-contract."""

from __future__ import annotations

import streamlit as st

from queries import calibration_data


def render_calibration(conn, start_utc: str, end_utc: str) -> None:
    st.subheader("Calibration")
    df = calibration_data(conn, start_utc, end_utc)
    if df.empty:
        st.info("No closed positions with edge-at-entry in this range.")
        return

    plot_df = df.copy()
    plot_df["edge_at_entry_pct"] = plot_df["edge_at_entry"].astype(float) * 100.0
    plot_df["pnl_per_contract_$"] = plot_df["pnl_per_contract"].astype(float)
    plot_df["signal_type"] = plot_df["signal_type"].fillna("(none)")

    try:
        import plotly.express as px

        fig = px.scatter(
            plot_df,
            x="edge_at_entry_pct",
            y="pnl_per_contract_$",
            color="signal_type",
            color_discrete_map={"BUY_YES": "#1f77b4", "SELL_YES": "#d62728"},
            trendline="lowess",
            trendline_scope="overall",
            hover_data=["pnl_net", "contracts"],
            labels={
                "edge_at_entry_pct": "Edge at entry (%)",
                "pnl_per_contract_$": "P&L per contract ($)",
                "signal_type": "Signal type",
            },
        )
        fig.add_hline(y=0, line_dash="dot", line_color="gray")
        fig.update_yaxes(tickformat="$.2f")
        fig.update_layout(height=420, margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(fig, use_container_width=True)
    except Exception as exc:  # plotly or statsmodels missing
        st.warning(f"Plotly trendline unavailable ({exc}). Falling back to table view.")
        st.dataframe(plot_df, use_container_width=True, hide_index=True)
