#!/usr/bin/env python3
"""
Paul (ops-layer) — intraday truncation edge alert (paper-only).

Triggers when:
- today's latest intraday snapshot used observed-max truncation, and
- there is still meaningful market price on dead (model_prob==0) brackets.

Deduping:
- alert at most once per (event_date, observed_max_f_at_snapshot) level.
"""

import sqlite3
import sys
from datetime import date, datetime, timezone

import requests


DB = "/home/ubuntu/nyc-temp-model/nyc_temp_log.sqlite"
BOT_TOKEN = "8652874695:AAFie5ef1mj7YXFeCs1yFiDqOEO4A76Ekg4"
CHAT_ID = -5229782521

DEAD_BRACKET_VALUE_THRESHOLD = 0.10  # PROVISIONAL (10c)
DEAD_BRACKET_PRICE_MIN = 0.02  # PROVISIONAL (2c)


def _utc_now_s() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> int:
    conn = sqlite3.connect(DB)
    try:
        today = date.today().isoformat()
        latest = conn.execute(
            """
            SELECT MAX(snapshot_ts)
            FROM bracket_snapshots
            WHERE snapshot_type='intraday' AND event_date=?
            """,
            (today,),
        ).fetchone()
        if not latest or not latest[0]:
            return 0
        snap_ts = str(latest[0])

        # Need truncation active
        om = conn.execute(
            """
            SELECT MAX(observed_max_f_at_snapshot)
            FROM bracket_snapshots
            WHERE snapshot_type='intraday' AND event_date=? AND snapshot_ts=?
            """,
            (today, snap_ts),
        ).fetchone()
        if not om or om[0] is None:
            return 0
        observed_max = float(om[0])

        # Dedup table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS paul_alerts (
              alert_key TEXT PRIMARY KEY,
              created_ts TEXT NOT NULL
            )
            """
        )
        alert_key = f"trunc:{today}:{observed_max:.1f}"
        already = conn.execute("SELECT 1 FROM paul_alerts WHERE alert_key=?", (alert_key,)).fetchone()
        if already:
            return 0

        rows = conn.execute(
            """
            SELECT bracket_label, model_prob, market_price, edge,
                   hrrr_max_f, hrrr_shift_applied_f
            FROM bracket_snapshots
            WHERE snapshot_type='intraday' AND event_date=? AND snapshot_ts=?
            ORDER BY bracket_lower_f
            """,
            (today, snap_ts),
        ).fetchall()
        if not rows:
            return 0

        dead = [(r[0], float(r[2] or 0.0)) for r in rows if (r[1] == 0.0 and (r[2] or 0.0) > DEAD_BRACKET_PRICE_MIN)]
        dead_value = sum(p for _, p in dead)
        best = max(rows, key=lambda r: (r[3] if r[3] is not None else -999))

        if dead_value < DEAD_BRACKET_VALUE_THRESHOLD:
            return 0

        # Mark sent before posting to minimize duplicates on retries.
        conn.execute(
            "INSERT OR IGNORE INTO paul_alerts(alert_key, created_ts) VALUES (?, ?)",
            (alert_key, _utc_now_s()),
        )
        conn.commit()

        hrrr_max = rows[0][4]
        hrrr_shift_applied = rows[0][5]
        lines = [
            f"Intraday truncation edge — {today}",
            f"Running high: {observed_max:.1f}°F as of {snap_ts}",
            "",
            "Dead brackets still priced:",
        ]
        for lab, p in dead[:12]:
            lines.append(f"  {lab}: {int(round(p*100))}c")
        if len(dead) > 12:
            lines.append(f"  (+{len(dead)-12} more)")
        lines.append("")
        lines.append(f"Total mispriced value: {int(round(dead_value*100))}c")
        lines.append(
            f"Best live bracket: {best[0]} model {int(round((best[1] or 0.0)*100))}% "
            f"mkt {int(round((best[2] or 0.0)*100))}% edge {best[3]*100:+.1f}%"
        )
        if hrrr_max is not None:
            shift_s = "" if hrrr_shift_applied is None else f" (applied {float(hrrr_shift_applied):+.1f}F)"
            lines.append(f"HRRR daily max: {float(hrrr_max):.1f}°F{shift_s}")

        requests.post(
            "https://api.telegram.org/bot" + BOT_TOKEN + "/sendMessage",
            json={"chat_id": CHAT_ID, "text": "\n".join(lines)},
            timeout=20,
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

