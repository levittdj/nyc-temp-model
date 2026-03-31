#!/usr/bin/env python3
"""
Post-canary sanity checks for v0 SQLite data quality.

Run:
  python3 scripts/post_canary_sanity.py --db nyc_temp_log.sqlite
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def _count(conn: sqlite3.Connection, sql: str, args: tuple = ()) -> int:
    row = conn.execute(sql, args).fetchone()
    return int(row[0]) if row else 0


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, default=Path("nyc_temp_log.sqlite"))
    args = ap.parse_args()

    conn = sqlite3.connect(str(args.db))
    try:
        checks: list[tuple[str, bool, str]] = []

        has_dsm = _table_exists(conn, "dsm_observations")
        has_cli = _table_exists(conn, "cli_observations")

        if has_dsm:
            dsm_rows = _count(conn, "SELECT COUNT(*) FROM dsm_observations")
            checks.append(
                ("dsm_rows_exist", dsm_rows > 0, f"dsm_observations rows={dsm_rows}")
            )
            bad_dsm = _count(
                conn,
                """
                SELECT COUNT(*)
                FROM dsm_observations
                WHERE running_high_f IS NULL OR event_date IS NULL OR fetch_ts IS NULL
                """,
            )
            checks.append(
                ("dsm_required_fields", bad_dsm == 0, f"bad dsm rows={bad_dsm}")
            )
        else:
            checks.append(
                ("dsm_table_present", False, "dsm_observations table is missing")
            )

        if has_cli:
            cli_rows = _count(conn, "SELECT COUNT(*) FROM cli_observations")
            checks.append(
                ("cli_rows_exist", cli_rows > 0, f"cli_observations rows={cli_rows}")
            )
            bad_cli = _count(
                conn,
                """
                SELECT COUNT(*)
                FROM cli_observations
                WHERE cli_high_f IS NULL OR event_date IS NULL OR fetch_ts IS NULL
                """,
            )
            checks.append(
                ("cli_required_fields", bad_cli == 0, f"bad cli rows={bad_cli}")
            )
        else:
            checks.append(
                ("cli_table_present", False, "cli_observations table is missing")
            )

        bad_winners = _count(
            conn,
            """
            SELECT COUNT(*)
            FROM (
              SELECT event_date, SUM(CASE WHEN outcome=1 THEN 1 ELSE 0 END) AS winners
              FROM bracket_snapshots
              WHERE actual_max_f IS NOT NULL
              GROUP BY event_date
            )
            WHERE winners <> 1
            """,
        )
        checks.append(
            (
                "single_winner_per_backfilled_date",
                bad_winners == 0,
                f"event_dates with winners!=1: {bad_winners}",
            )
        )

        missing_backfills = _count(
            conn,
            """
            SELECT COUNT(DISTINCT event_date)
            FROM bracket_snapshots
            WHERE event_date < date('now','-1 day')
              AND (actual_max_f IS NULL OR outcome IS NULL)
            """,
        )
        checks.append(
            (
                "missing_backfills_older_than_1d",
                missing_backfills == 0,
                f"missing backfill dates={missing_backfills}",
            )
        )

        all_ok = True
        for name, ok, detail in checks:
            status = "PASS" if ok else "FAIL"
            print(f"{status:4}  {name:36}  {detail}")
            if not ok:
                all_ok = False

        return 0 if all_ok else 2
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
