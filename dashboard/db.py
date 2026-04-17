"""
Read-only SQLite connection for the dashboard.

Why read-only: collector.py and logger.py (backfill) are the only writers.
The dashboard must never race them for the WAL lock or accidentally mutate
data.  Opening with ?mode=ro at the URI level makes writes a hard error
rather than a silent concurrency bug.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


def get_ro_connection(db_path: Path) -> sqlite3.Connection:
    """Open db_path in SQLite read-only mode (URI ?mode=ro)."""
    uri = f"file:{db_path.resolve()}?mode=ro"
    return sqlite3.connect(uri, uri=True)
