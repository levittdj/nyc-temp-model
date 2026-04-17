"""
NYC Temp Paper Trading dashboard — v0 review only.

No live execution. Read-only DB access.
"""

from __future__ import annotations

import os
from pathlib import Path

import streamlit as st

from db import get_ro_connection
from queries import latest_snapshot_ts

_DEFAULT_DB = Path(__file__).resolve().parent.parent / "nyc_temp_log.sqlite"
DB_PATH = Path(os.environ.get("NYC_TEMP_DB", str(_DEFAULT_DB)))

st.set_page_config(page_title="NYC Temp Paper Trading", layout="wide")
st.title("NYC Temp Paper Trading \u2014 v0 review")

if not DB_PATH.exists():
    st.error(f"Database not found: {DB_PATH}")
    st.stop()

conn = get_ro_connection(DB_PATH)
ts = latest_snapshot_ts(conn)
conn.close()

if ts:
    st.metric("Latest snapshot", ts)
else:
    st.warning("No snapshots found in the database.")
