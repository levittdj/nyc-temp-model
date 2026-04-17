"""
All SQL for the dashboard lives here.

If logger.py SCHEMA changes (new columns, renamed tables, altered types),
update the queries in this module.  Do not inline SQL in app.py or sections.py.

Queries return pandas DataFrames for table-shaped results and scalars for
single-value lookups.  Date-range queries expect UTC ISO-8601 Z strings
(see dashboard/filters.py) and filter on entry_ts unless documented otherwise.

Where a query mirrors a canned query in metadata.yml, the Datasette query
name is called out in a comment so the two stay in sync.
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


def open_positions_with_unrealized(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Open paper_positions joined with the latest bracket_snapshots row for that
    event_date+bracket_label (KXHIGHNY).  Unrealized P&L follows the exact formula
    in scripts/paul_price_move.py — keep the two in sync:
        YES side: contracts * (current_mkt - avg_entry_price)
        NO  side: contracts * ((1 - current_mkt) - avg_entry_price)
    """
    sql = """
        SELECT p.event_date, p.bracket_label, p.side, p.contracts,
               p.avg_entry_price, p.entry_ts, p.entry_signal_id,
               s.reason AS signal_reason, s.signal_type,
               latest.market_price AS current_mkt,
               latest.hours_to_settle AS hours_to_settle,
               latest.snapshot_ts AS latest_snapshot_ts
        FROM paper_positions p
        LEFT JOIN intraday_signals s ON s.signal_id = p.entry_signal_id
        LEFT JOIN (
            SELECT event_date, bracket_label, market_price, hours_to_settle, snapshot_ts,
                   ROW_NUMBER() OVER (
                       PARTITION BY event_date, bracket_label
                       ORDER BY snapshot_ts DESC
                   ) AS rn
            FROM bracket_snapshots
            WHERE COALESCE(series_ticker, 'KXHIGHNY') = 'KXHIGHNY'
        ) latest
          ON latest.event_date = p.event_date
         AND latest.bracket_label = p.bracket_label
         AND latest.rn = 1
        WHERE p.status = 'open'
        ORDER BY p.event_date, p.bracket_label
    """
    df = pd.read_sql_query(sql, conn)
    if df.empty:
        df["unrealized_pnl"] = []
        return df
    # paul_price_move.py uses case-sensitive 'YES'; uppercase defensively.
    side_u = df["side"].fillna("").str.upper()
    aep = df["avg_entry_price"].astype(float)
    n = df["contracts"].astype(float)
    mkt = pd.to_numeric(df["current_mkt"], errors="coerce")
    yes_pnl = n * (mkt - aep)
    no_pnl = n * ((1.0 - mkt) - aep)
    df["unrealized_pnl"] = yes_pnl.where(side_u == "YES", no_pnl)
    return df


# Mirrors metadata.yml: paper_signal_decomposition (keep CASE logic identical).
def signal_decomposition(conn: sqlite3.Connection) -> pd.DataFrame:
    """Signal source buckets across all closed paper_positions."""
    sql = """
        SELECT
          CASE
            WHEN s.reason LIKE 'dead bracket (truncation%' THEN 'truncation'
            WHEN s.reason LIKE 'dead bracket (model%'      THEN 'model_dead'
            WHEN s.hrrr_shift_f IS NOT NULL
              AND ABS(s.hrrr_shift_f) > 0.5              THEN 'hrrr_driven'
            WHEN s.trajectory_deviation_f IS NOT NULL
              AND ABS(s.trajectory_deviation_f) > 1.0    THEN 'trajectory_driven'
            WHEN s.ensemble_ratio IS NOT NULL
              AND ABS(s.ensemble_ratio - 1.0) > 0.15     THEN 'ensemble_driven'
            ELSE 'edge_only'
          END AS signal_source,
          COUNT(*) AS n,
          ROUND(AVG(p.pnl_net), 4) AS mean_pnl,
          ROUND(SUM(p.pnl_net), 4) AS total_pnl,
          SUM(CASE WHEN p.pnl_net > 0 THEN 1 ELSE 0 END) AS wins,
          SUM(CASE WHEN p.pnl_net <= 0 THEN 1 ELSE 0 END) AS losses
        FROM intraday_signals s
        JOIN paper_positions p ON p.entry_signal_id = s.signal_id
        WHERE p.status IN ('exited', 'settled')
        GROUP BY signal_source
        ORDER BY total_pnl DESC
    """
    return pd.read_sql_query(sql, conn)


def signal_type_breakdown(conn: sqlite3.Connection) -> pd.DataFrame:
    """Per-signal_type (BUY_YES / SELL_YES / EXIT) stats — BUY_YES and SELL_YES NOT pooled."""
    sql = """
        SELECT
          s.signal_type,
          COUNT(*) AS n,
          SUM(CASE WHEN p.pnl_net > 0  THEN 1 ELSE 0 END) AS wins,
          SUM(CASE WHEN p.pnl_net <= 0 THEN 1 ELSE 0 END) AS losses,
          AVG(CASE WHEN p.pnl_net > 0  THEN p.pnl_net END) AS avg_winner,
          AVG(CASE WHEN p.pnl_net <= 0 THEN p.pnl_net END) AS avg_loser,
          AVG(s.edge) AS mean_edge_at_entry
        FROM intraday_signals s
        JOIN paper_positions p ON p.entry_signal_id = s.signal_id
        WHERE p.status IN ('exited', 'settled')
        GROUP BY s.signal_type
        ORDER BY s.signal_type
    """
    return pd.read_sql_query(sql, conn)


def fee_details(conn: sqlite3.Connection) -> dict:
    """Aggregate fee accounting across all closed positions (sanity check vs intraday_engine.estimate_fee)."""
    row = conn.execute(
        """
        SELECT COALESCE(SUM(entry_fee), 0) AS total_entry_fees,
               COALESCE(SUM(exit_fee),  0) AS total_exit_fees,
               COALESCE(SUM(pnl_gross), 0) AS total_gross,
               COALESCE(SUM(contracts), 0) AS total_contracts
        FROM paper_positions
        WHERE status IN ('exited','settled')
        """
    ).fetchone()
    total_entry = float(row[0] or 0.0)
    total_exit = float(row[1] or 0.0)
    total_gross = float(row[2] or 0.0)
    total_contracts = int(row[3] or 0)
    total_fees = total_entry + total_exit
    fee_drag_pct = total_fees / max(abs(total_gross), 0.01) * 100.0
    avg_fee_per_contract = total_fees / total_contracts if total_contracts else 0.0
    return {
        "total_entry_fees": total_entry,
        "total_exit_fees": total_exit,
        "total_fees": total_fees,
        "total_gross": total_gross,
        "total_contracts": total_contracts,
        "fee_drag_pct": fee_drag_pct,
        "avg_fee_per_contract": avg_fee_per_contract,
    }
