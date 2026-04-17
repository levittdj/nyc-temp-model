"""
All SQL for the dashboard lives here.

If the logger.py SCHEMA changes (new columns, renamed tables, altered types),
update the queries in this module.  Do not inline SQL in app.py or charts.py.
"""

from __future__ import annotations

import sqlite3
from typing import Optional


def latest_snapshot_ts(conn: sqlite3.Connection) -> Optional[str]:
    """Most recent snapshot_ts across all rows in bracket_snapshots."""
    row = conn.execute(
        "SELECT MAX(snapshot_ts) FROM bracket_snapshots"
    ).fetchone()
    return row[0] if row and row[0] else None
