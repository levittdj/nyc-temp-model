"""NBM revisions: morning p50 vs latest intraday p50 per event_date."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from queries import nbm_revisions_in_range
from style import section_header

from ._format import to_et

# PROVISIONAL: matches NBM_SHIFT_THRESHOLD_F in scripts/paul_morning_edge.py.
# If either moves, update both together.
NBM_REVISION_FLAG_F: float = 2.0


def render_nbm_revisions(conn, start_utc: str, end_utc: str) -> None:
    section_header("REVISIONS", "NBM revisions")
    df = nbm_revisions_in_range(conn, start_utc, end_utc)
    if df.empty:
        st.info("No morning NBM snapshots in this range.")
        return

    show = pd.DataFrame({
        "event_date": df["event_date"],
        "morning p50 (F)": df["morning_p50"],
        "latest intraday p50 (F)": df["latest_p50"],
        "delta (F)": df["delta_f"],
        "first revision \u2265 2F (ET)": df["first_rev_ts"].map(to_et),
        "did we trade on revised distribution?": df["trades_after_revision"].fillna(0).astype(int),
    })

    def _highlight(row):
        try:
            flagged = abs(float(row["delta (F)"])) >= NBM_REVISION_FLAG_F
        except (TypeError, ValueError):
            flagged = False
        bg = "background-color: #fff3cd" if flagged else ""
        return [bg] * len(row)

    st.caption(
        f"Highlighted rows: |delta| \u2265 {NBM_REVISION_FLAG_F}\u00b0F "
        f"(PROVISIONAL; same threshold as scripts/paul_morning_edge.py)."
    )
    st.dataframe(show.style.apply(_highlight, axis=1),
                 use_container_width=True, hide_index=True)
