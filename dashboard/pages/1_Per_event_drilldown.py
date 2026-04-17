"""
Per-event drill-down page.  Streamlit auto-registers this under the sidebar
navigation because it lives in dashboard/pages/.

Strictly read-only.  No caching.  All SQL in dashboard/queries.py.
"""

from __future__ import annotations

import os
from pathlib import Path

import streamlit as st

from db import get_ro_connection
from queries import event_dates_with_paper_positions
from sections import render_event_drilldown
from style import inject_global_css

_DEFAULT_DB = Path(__file__).resolve().parents[2] / "nyc_temp_log.sqlite"
DB_PATH = Path(os.environ.get("NYC_TEMP_DB", str(_DEFAULT_DB)))

st.set_page_config(page_title="Per-event drill-down", layout="wide")
inject_global_css()
st.title("Per-event drill-down")

if not DB_PATH.exists():
    st.error(f"Database not found: {DB_PATH}")
    st.stop()

conn = get_ro_connection(DB_PATH)
event_dates = event_dates_with_paper_positions(conn)

if not event_dates:
    st.info("No event_dates with paper_positions yet.  Once the intraday_engine "
            "writes a position, it will appear here.")
    conn.close()
    st.stop()

event_date = st.selectbox(
    "Event date", options=event_dates, index=0,
    help="Only event_dates with at least one paper_position row are listed.",
)

render_event_drilldown(conn, event_date)
conn.close()
