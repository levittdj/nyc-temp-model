"""Equity curve, trade log, open positions."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from queries import open_positions_with_unrealized
from style import section_header

from ._format import ET, dollars, hours_to_settle_live, to_et

_USD = st.column_config.NumberColumn(format="$%.2f")


def _append_total_row(display: pd.DataFrame,
                      label_col: str,
                      numeric_cols: list[str]) -> pd.DataFrame:
    """Append a TOTAL row summing ``numeric_cols``; other columns left blank.

    Kept visually in the same table so NumberColumn formatting still applies.
    """
    if display.empty:
        return display
    total: dict[str, object] = {c: pd.NA for c in display.columns}
    total[label_col] = "TOTAL"
    for c in numeric_cols:
        if c in display.columns:
            total[c] = pd.to_numeric(display[c], errors="coerce").sum(min_count=1)
    return pd.concat([display, pd.DataFrame([total])], ignore_index=True)


def render_equity_curve(closed: pd.DataFrame) -> None:
    section_header("OVERVIEW", "Equity curve")
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
    section_header("TRADES", "Trade log")
    if trades.empty:
        st.info("No trades in this range.")
        return
    t = trades.copy()
    t["entry (ET)"] = t["entry_ts"].map(to_et)
    t["entry $"] = t["avg_entry_price"].astype(float)
    t["exit $"] = pd.to_numeric(t["exit_price"], errors="coerce")
    t["outlay $"] = t["contracts"].astype(float) * t["avg_entry_price"].astype(float)
    t["gross $"] = t["pnl_gross"].fillna(0).astype(float)
    t["net $"] = t["pnl_net"].fillna(0).astype(float)
    cols = [
        "event_date", "bracket_label", "side", "contracts",
        "entry (ET)", "entry $", "exit $", "outlay $",
        "settlement_outcome", "status", "gross $", "net $",
        "signal_reason",
    ]
    show = t[cols].rename(columns={
        "bracket_label": "bracket",
        "settlement_outcome": "settled",
        "signal_reason": "reason",
    })
    show = _append_total_row(
        show,
        label_col="event_date",
        numeric_cols=["contracts", "outlay $", "gross $", "net $"],
    )
    st.dataframe(
        show,
        use_container_width=True,
        hide_index=True,
        column_config={
            "entry $": _USD, "exit $": _USD, "outlay $": _USD,
            "gross $": _USD, "net $": _USD,
        },
    )


def render_open_positions(conn) -> None:
    section_header("POSITIONS", "Open positions")
    op = open_positions_with_unrealized(conn)
    if op.empty:
        st.info("No open positions.")
        return
    display = pd.DataFrame({
        "event_date": op["event_date"],
        "bracket": op["bracket_label"],
        "side": op["side"],
        "contracts": op["contracts"].astype(float),
        "entry $": op["avg_entry_price"].astype(float),
        "current $": pd.to_numeric(op["current_mkt"], errors="coerce"),
        "outlay $": (op["contracts"].astype(float)
                     * op["avg_entry_price"].astype(float)),
        "unrealized $": op["unrealized_pnl"].astype(float),
        # Live hrs-to-settle from event_date; the snapshot-column value can
        # be hours stale between collector ticks.  See _format.hours_to_settle_live.
        "hrs to settle": op["event_date"].map(hours_to_settle_live).astype(float).round(1),
        "reason": op["signal_reason"],
    })
    display = _append_total_row(
        display,
        label_col="event_date",
        numeric_cols=["contracts", "outlay $", "unrealized $"],
    )
    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "entry $": _USD, "current $": _USD,
            "outlay $": _USD, "unrealized $": _USD,
        },
    )
