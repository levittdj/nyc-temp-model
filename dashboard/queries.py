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


def today_high(
    conn: sqlite3.Connection, event_date: str
) -> tuple[Optional[float], str, Optional[str]]:
    """
    Best available observed high for event_date.
    Priority: actual_max_f (backfilled) → dsm running_high_f → observed_max_f_at_snapshot.
    Returns (value_or_None, source_label, timestamp_utc_or_None).
    """
    # 1. Settled/backfilled — NYC only
    row = conn.execute(
        "SELECT actual_max_f, MAX(snapshot_ts) FROM bracket_snapshots WHERE event_date=? AND series_ticker='KXHIGHNY' AND actual_max_f IS NOT NULL",
        (event_date,),
    ).fetchone()
    if row and row[0] is not None:
        return float(row[0]), "final", row[1]

    # 2. DSM official running high — fetch the issuance_ts of the max row
    row = conn.execute(
        """SELECT running_high_f, issuance_ts FROM dsm_observations
           WHERE event_date=? ORDER BY running_high_f DESC LIMIT 1""",
        (event_date,),
    ).fetchone()
    if row and row[0] is not None:
        return float(row[0]), "DSM", row[1]

    # 3. Collector intraday observed max (least authoritative) — NYC only
    # Use MAX(snapshot_ts) so the timestamp reflects the last collector run, not when the max was first hit.
    row = conn.execute(
        """SELECT MAX(observed_max_f_at_snapshot), MAX(snapshot_ts) FROM bracket_snapshots
           WHERE event_date=? AND series_ticker='KXHIGHNY' AND observed_max_f_at_snapshot IS NOT NULL""",
        (event_date,),
    ).fetchone()
    if row and row[0] is not None:
        return float(row[0]), "intraday", row[1]

    return None, "", None


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


def event_dates_with_paper_positions(conn: sqlite3.Connection) -> list:
    """Distinct event_dates that have at least one paper_position row (any status)."""
    rows = conn.execute(
        "SELECT DISTINCT event_date FROM paper_positions ORDER BY event_date DESC"
    ).fetchall()
    return [r[0] for r in rows]


def event_day_summary(conn: sqlite3.Connection, event_date: str) -> dict:
    """Header summary for the per-event drilldown: actual high, winning bracket, p50 shift, day P&L."""
    actual_row = conn.execute(
        "SELECT MAX(actual_max_f) FROM bracket_snapshots WHERE event_date=?",
        (event_date,),
    ).fetchone()
    winning_row = conn.execute(
        "SELECT bracket_label FROM bracket_snapshots WHERE event_date=? AND outcome=1 LIMIT 1",
        (event_date,),
    ).fetchone()
    morning_row = conn.execute(
        """
        SELECT nbm_p50_adj FROM bracket_snapshots
        WHERE event_date=? AND snapshot_type='morning' AND nbm_p50_adj IS NOT NULL
        ORDER BY snapshot_ts LIMIT 1
        """,
        (event_date,),
    ).fetchone()
    final_row = conn.execute(
        """
        SELECT nbm_p50_adj FROM bracket_snapshots
        WHERE event_date=? AND snapshot_type='intraday' AND nbm_p50_adj IS NOT NULL
        ORDER BY snapshot_ts DESC LIMIT 1
        """,
        (event_date,),
    ).fetchone()
    pnl_row = conn.execute(
        """
        SELECT COALESCE(SUM(pnl_net), 0) FROM paper_positions
        WHERE event_date=? AND status IN ('exited','settled')
        """,
        (event_date,),
    ).fetchone()
    return {
        "actual_max_f": actual_row[0] if actual_row and actual_row[0] is not None else None,
        "winning_bracket": winning_row[0] if winning_row else None,
        "morning_p50": morning_row[0] if morning_row and morning_row[0] is not None else None,
        "final_p50": final_row[0] if final_row and final_row[0] is not None else None,
        "pnl_net": float(pnl_row[0]) if pnl_row and pnl_row[0] is not None else 0.0,
    }


def bracket_price_trajectory(
    conn: sqlite3.Connection, event_date: str
) -> pd.DataFrame:
    """Per-bracket intraday price + model_prob timeline for one event_date (KXHIGHNY only)."""
    sql = """
        SELECT snapshot_ts, bracket_label, market_price, model_prob
        FROM bracket_snapshots
        WHERE event_date = ?
          AND snapshot_type = 'intraday'
          AND COALESCE(series_ticker, 'KXHIGHNY') = 'KXHIGHNY'
        ORDER BY snapshot_ts, bracket_label
    """
    return pd.read_sql_query(sql, conn, params=(event_date,))


def dsm_running_high(conn: sqlite3.Connection, event_date: str) -> pd.DataFrame:
    """Running observed-high track from DSM observations, for overlay on the drilldown chart."""
    sql = """
        SELECT fetch_ts, running_high_f
        FROM dsm_observations
        WHERE event_date = ?
        ORDER BY fetch_ts
    """
    return pd.read_sql_query(sql, conn, params=(event_date,))


def event_day_signals(conn: sqlite3.Connection, event_date: str) -> pd.DataFrame:
    """All intraday_signals (executed AND blocked) for one event_date."""
    sql = """
        SELECT snapshot_ts, bracket_label, signal_type, executed,
               reason, contracts_suggested, edge
        FROM intraday_signals
        WHERE event_date = ?
        ORDER BY snapshot_ts
    """
    return pd.read_sql_query(sql, conn, params=(event_date,))


def event_day_positions(conn: sqlite3.Connection, event_date: str) -> pd.DataFrame:
    """All paper_positions for one event_date."""
    sql = """
        SELECT bracket_label, side, contracts, entry_ts,
               avg_entry_price, exit_price, status, pnl_net
        FROM paper_positions
        WHERE event_date = ?
        ORDER BY entry_ts
    """
    return pd.read_sql_query(sql, conn, params=(event_date,))


def calibration_data(
    conn: sqlite3.Connection, start_utc: str, end_utc: str
) -> pd.DataFrame:
    """Edge-at-entry vs pnl_net-per-contract for every closed position in range."""
    sql = """
        SELECT p.pnl_net, p.contracts,
               s.edge AS edge_at_entry,
               s.signal_type
        FROM paper_positions p
        JOIN intraday_signals s ON s.signal_id = p.entry_signal_id
        WHERE p.status IN ('exited', 'settled')
          AND p.entry_ts >= ? AND p.entry_ts < ?
          AND s.edge IS NOT NULL
          AND p.contracts > 0
    """
    df = pd.read_sql_query(sql, conn, params=(start_utc, end_utc))
    if df.empty:
        df["pnl_per_contract"] = []
        return df
    df["pnl_per_contract"] = df["pnl_net"].astype(float) / df["contracts"].astype(float)
    return df


def nbm_revisions_in_range(
    conn: sqlite3.Connection, start_utc: str, end_utc: str
) -> pd.DataFrame:
    """
    Morning p50_adj vs latest intraday p50_adj per event_date; flags revisions
    >= 2F and counts paper_positions entered AFTER the first revision snapshot.

    2F threshold is PROVISIONAL — mirrors NBM_SHIFT_THRESHOLD_F in
    scripts/paul_morning_edge.py.  Keep the two in sync if the threshold moves.
    """
    start_date = start_utc[:10]
    end_date = end_utc[:10]
    sql = """
        WITH morning AS (
          SELECT event_date, AVG(nbm_p50_adj) AS morning_p50
          FROM bracket_snapshots
          WHERE snapshot_type='morning' AND nbm_p50_adj IS NOT NULL
          GROUP BY event_date
        ),
        latest AS (
          SELECT b.event_date, b.nbm_p50_adj AS latest_p50, b.snapshot_ts AS latest_ts
          FROM bracket_snapshots b
          WHERE b.snapshot_type='intraday' AND b.nbm_p50_adj IS NOT NULL
            AND b.snapshot_ts = (
                SELECT MAX(snapshot_ts) FROM bracket_snapshots
                WHERE event_date = b.event_date
                  AND snapshot_type='intraday'
                  AND nbm_p50_adj IS NOT NULL
            )
        ),
        first_revision AS (
          SELECT b.event_date, MIN(b.snapshot_ts) AS first_rev_ts
          FROM bracket_snapshots b
          JOIN morning m ON m.event_date = b.event_date
          WHERE b.snapshot_type='intraday' AND b.nbm_p50_adj IS NOT NULL
            AND ABS(b.nbm_p50_adj - m.morning_p50) >= 2.0
          GROUP BY b.event_date
        )
        SELECT m.event_date,
               ROUND(m.morning_p50, 2) AS morning_p50,
               ROUND(l.latest_p50, 2)  AS latest_p50,
               ROUND(l.latest_p50 - m.morning_p50, 2) AS delta_f,
               fr.first_rev_ts,
               (SELECT COUNT(*) FROM paper_positions p
                 WHERE p.event_date = m.event_date
                   AND fr.first_rev_ts IS NOT NULL
                   AND p.entry_ts > fr.first_rev_ts) AS trades_after_revision
        FROM morning m
        LEFT JOIN latest l ON l.event_date = m.event_date
        LEFT JOIN first_revision fr ON fr.event_date = m.event_date
        WHERE m.event_date >= ? AND m.event_date < ?
        ORDER BY m.event_date DESC
    """
    return pd.read_sql_query(sql, conn, params=(start_date, end_date))


def blocked_signals_by_reason(
    conn: sqlite3.Connection, start_utc: str, end_utc: str
) -> pd.DataFrame:
    """Counts of blocked intraday_signals grouped by reason, within [start, end)."""
    sql = """
        SELECT COALESCE(reason, '(none)') AS reason, COUNT(*) AS n
        FROM intraday_signals
        WHERE executed = 0
          AND snapshot_ts >= ? AND snapshot_ts < ?
        GROUP BY reason
        ORDER BY n DESC
    """
    return pd.read_sql_query(sql, conn, params=(start_utc, end_utc))


def blocked_signals_recent(
    conn: sqlite3.Connection, start_utc: str, end_utc: str, limit: int = 20
) -> pd.DataFrame:
    """Most-recent blocked intraday_signals within [start, end)."""
    sql = """
        SELECT snapshot_ts, bracket_label, signal_type, reason,
               edge, model_prob, market_price
        FROM intraday_signals
        WHERE executed = 0
          AND snapshot_ts >= ? AND snapshot_ts < ?
        ORDER BY snapshot_ts DESC
        LIMIT ?
    """
    return pd.read_sql_query(sql, conn, params=(start_utc, end_utc, limit))


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
