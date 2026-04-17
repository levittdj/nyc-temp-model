"""
All SQL for the dashboard lives here.

If the logger.py SCHEMA changes (new columns, renamed tables, altered types),
update the queries in this module.  Do not inline SQL in app.py or charts.py.

Queries return pandas DataFrames for table-shaped results and scalars for
single-value lookups.  Date-range queries expect UTC ISO-8601 Z strings
(see dashboard/filters.py) and filter on entry_ts unless documented otherwise.
"""

from __future__ import annotations

import sqlite3
from typing import Optional

import pandas as pd


def latest_snapshot_ts(conn: sqlite3.Connection) -> Optional[str]:
    """Most recent snapshot_ts across all rows in bracket_snapshots."""
    row = conn.execute("SELECT MAX(snapshot_ts) FROM bracket_snapshots").fetchone()
    return row[0] if row and row[0] else None


def closed_positions_in_range(
    conn: sqlite3.Connection, start_utc: str, end_utc: str
) -> pd.DataFrame:
    """Closed paper_positions (status IN exited/settled) with entry_ts in [start, end)."""
    sql = """
        SELECT position_id, event_date, bracket_label, side, contracts,
               avg_entry_price, entry_ts, entry_fee,
               exit_price, exit_ts, exit_fee,
               settlement_outcome, status,
               pnl_gross, pnl_net
        FROM paper_positions
        WHERE status IN ('exited','settled')
          AND entry_ts >= ? AND entry_ts < ?
        ORDER BY COALESCE(exit_ts, entry_ts)
    """
    return pd.read_sql_query(sql, conn, params=(start_utc, end_utc))


def trades_in_range(
    conn: sqlite3.Connection, start_utc: str, end_utc: str
) -> pd.DataFrame:
    """All paper_positions (open+closed) joined with entry signal reason."""
    sql = """
        SELECT p.position_id, p.event_date, p.bracket_label, p.side, p.contracts,
               p.entry_ts, p.avg_entry_price,
               p.exit_ts, p.exit_price, p.settlement_outcome, p.status,
               p.pnl_gross, p.pnl_net,
               s.reason AS signal_reason, s.signal_type
        FROM paper_positions p
        LEFT JOIN intraday_signals s ON s.signal_id = p.entry_signal_id
        WHERE p.entry_ts >= ? AND p.entry_ts < ?
        ORDER BY p.entry_ts DESC
    """
    return pd.read_sql_query(sql, conn, params=(start_utc, end_utc))


def cumulative_net_pnl_all_time(conn: sqlite3.Connection) -> float:
    """Sum(pnl_net) over all closed positions, no date filter."""
    row = conn.execute(
        """
        SELECT COALESCE(SUM(pnl_net), 0)
        FROM paper_positions
        WHERE status IN ('exited','settled')
        """
    ).fetchone()
    return float(row[0]) if row and row[0] is not None else 0.0
