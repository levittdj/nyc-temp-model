#!/usr/bin/env python3
"""
Paul (ops-layer) — 7:00–7:30pm ET "lotto" alert.

Trigger (PROVISIONAL):
- For today's event_date, if the *second-highest* bracket market_price exceeds 5c
  at any point between 19:00 and 19:30 America/New_York, send a Telegram alert.

Deduping:
- alert at most once per event_date (stores key in paul_alerts table).
"""

import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone

import requests

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore


DB = "/home/ubuntu/nyc-temp-model/nyc_temp_log.sqlite"
BOT_TOKEN = "8652874695:AAFie5ef1mj7YXFeCs1yFiDqOEO4A76Ekg4"
CHAT_ID = -5229782521

SECOND_HIGHEST_THRESHOLD = 0.05  # PROVISIONAL (5c)


def _utc_z(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _et(ts) -> str:
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(ny).strftime('%-I:%M%p ET')


def main() -> int:
    ny = ZoneInfo("America/New_York")
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(ny)

    event_date = now_local.date()
    start_local = datetime(event_date.year, event_date.month, event_date.day, 19, 0, tzinfo=ny)
    end_local = start_local + timedelta(minutes=30)
    if not (start_local <= now_local <= end_local):
        return 0

    win_start_utc = start_local.astimezone(timezone.utc)
    win_end_utc = min(now_utc, end_local.astimezone(timezone.utc))

    conn = sqlite3.connect(DB)
    try:
        # Dedup table (shared with other Paul scripts)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS paul_alerts (
              alert_key TEXT PRIMARY KEY,
              created_ts TEXT NOT NULL
            )
            """
        )
        alert_key = f"730pm:{event_date.isoformat()}"
        if conn.execute("SELECT 1 FROM paul_alerts WHERE alert_key=?", (alert_key,)).fetchone():
            return 0

        # Pull snapshots in the window so far.
        snaps = conn.execute(
            """
            SELECT DISTINCT snapshot_ts
            FROM bracket_snapshots
            WHERE snapshot_type='intraday'
              AND event_date=?
              AND snapshot_ts >= ?
              AND snapshot_ts <= ?
            ORDER BY snapshot_ts
            """,
            (event_date.isoformat(), _utc_z(win_start_utc), _utc_z(win_end_utc)),
        ).fetchall()
        snap_ts_list = [str(r[0]) for r in snaps if r and r[0]]
        if not snap_ts_list:
            return 0

        best_second: float = -1.0
        best_ts: str = ""
        best_label: str = ""
        best_top: float = -1.0

        for ts in snap_ts_list:
            rows = conn.execute(
                """
                SELECT bracket_label, market_price
                FROM bracket_snapshots
                WHERE snapshot_type='intraday' AND event_date=? AND snapshot_ts=?
                  AND market_price IS NOT NULL
                """,
                (event_date.isoformat(), ts),
            ).fetchall()
            prices = [(str(lab), float(p)) for lab, p in rows if p is not None]
            if len(prices) < 2:
                continue
            prices.sort(key=lambda x: x[1], reverse=True)
            top_lab, top_p = prices[0]
            second_lab, second_p = prices[1]
            if second_p > best_second:
                best_second = second_p
                best_ts = ts
                best_label = second_lab
                best_top = top_p

        if best_second < SECOND_HIGHEST_THRESHOLD:
            return 0

        # Mark sent before posting to minimize duplicates on retries.
        conn.execute(
            "INSERT OR IGNORE INTO paul_alerts(alert_key, created_ts) VALUES (?, ?)",
            (alert_key, _utc_z(now_utc)),
        )
        conn.commit()

        msg = "\n".join(
            [
                f"7:00–7:30pm lotto check — {event_date.isoformat()}",
                f"Second-highest bracket traded >5c in window.",
                f"Peak 2nd-highest: {best_label} at {int(round(best_second*100))}c (top was {int(round(best_top*100))}c)",
                f"Snapshot: {_et(best_ts)}",
            ]
        )
        requests.post(
            "https://api.telegram.org/bot" + BOT_TOKEN + "/sendMessage",
            json={"chat_id": CHAT_ID, "text": msg},
            timeout=20,
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

