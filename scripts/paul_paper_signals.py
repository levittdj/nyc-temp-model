#!/usr/bin/env python3
"""
Paul (ops-layer) — paper signal alert.

Triggers when intraday_engine fired at least one executed signal in the latest
collector tick.  Sends a Telegram message listing each trade (bracket, side,
contracts, price, edge).

Dedup: one alert per (event_date, snapshot_ts) pair — fires at most once per
30-minute collector cycle when signals are present.
"""

import sqlite3
import sys
from datetime import date, datetime, timezone

import requests

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

DB = "/home/ubuntu/nyc-temp-model/nyc_temp_log.sqlite"
BOT_TOKEN = "8652874695:AAFie5ef1mj7YXFeCs1yFiDqOEO4A76Ekg4"
CHAT_ID = -5229782521

_NY = ZoneInfo("America/New_York")


def _et(ts_str: str) -> str:
    ts = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(_NY).strftime("%-I:%M%p ET")


def _utc_now_s() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> int:
    conn = sqlite3.connect(DB)
    try:
        today = date.today().isoformat()

        # Exit silently if paper tables don't exist yet
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        if 'intraday_signals' not in tables or 'paper_positions' not in tables:
            return 0

        # Find the latest snapshot_ts with at least one executed signal today
        row = conn.execute(
            """
            SELECT MAX(snapshot_ts)
            FROM intraday_signals
            WHERE event_date = ? AND executed = 1
            """,
            (today,),
        ).fetchone()
        if not row or not row[0]:
            return 0
        snap_ts = str(row[0])

        # Dedup table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS paul_alerts (
                alert_key TEXT PRIMARY KEY,
                created_ts TEXT NOT NULL
            )
            """
        )
        alert_key = f"paper_signals:{today}:{snap_ts}"
        if conn.execute("SELECT 1 FROM paul_alerts WHERE alert_key=?", (alert_key,)).fetchone():
            return 0

        # Fetch all executed signals for this snapshot
        signals = conn.execute(
            """
            SELECT bracket_label, signal_type, contracts_suggested, price_target,
                   edge, reason, kelly_full, model_prob, market_price
            FROM intraday_signals
            WHERE event_date = ? AND snapshot_ts = ? AND executed = 1
            ORDER BY bracket_label
            """,
            (today, snap_ts),
        ).fetchall()
        if not signals:
            return 0

        # Fetch open position summary
        positions = conn.execute(
            """
            SELECT bracket_label, side, contracts, avg_entry_price, status
            FROM paper_positions
            WHERE event_date = ?
            ORDER BY entry_ts DESC
            """,
            (today,),
        ).fetchall()
        open_count = sum(1 for p in positions if p[4] == "open")

        lines = [
            f"Paper signals — {today} @ {_et(snap_ts)}",
            "",
        ]
        for label, stype, contracts, price_target, edge, reason, kelly_full, model_prob, mkt_price in signals:
            side_str = "YES" if stype == "BUY_YES" else ("NO" if stype == "SELL_YES" else "EXIT")
            price_c = int(round(float(price_target or 0) * 100)) if price_target else 0
            edge_pct = f"{float(edge or 0)*100:+.1f}%" if edge is not None else "?"
            kelly_s = f"K={float(kelly_full or 0)*100:.0f}%" if kelly_full is not None else ""
            n = int(contracts or 0)
            cost_c = n * price_c
            payout_c = n * 100
            lines.append(
                f"  {stype} {label} ({side_str})"
                f"  {n}c @ {price_c}¢"
                f"  edge {edge_pct}"
                f"  {kelly_s}"
            )
            lines.append(f"    bet {cost_c}¢  payout {payout_c}¢  profit if correct {payout_c - cost_c}¢")
            if reason:
                lines.append(f"    → {reason}")

        lines.append("")
        lines.append(f"Open positions: {open_count}")

        # Cumulative P&L from all closed/settled positions
        pnl_row = conn.execute(
            """
            SELECT ROUND(SUM(pnl_net), 2), COUNT(*)
            FROM paper_positions
            WHERE status IN ('exited', 'settled')
            """
        ).fetchone()
        cum_pnl = pnl_row[0] if pnl_row and pnl_row[0] is not None else 0.0
        n_closed = pnl_row[1] if pnl_row else 0
        pnl_s = ('+' if cum_pnl >= 0 else '') + str(cum_pnl)
        lines.append(f"Cumulative P&L: {pnl_s}c  ({n_closed} closed)")

        # Mark sent before posting
        conn.execute(
            "INSERT OR IGNORE INTO paul_alerts(alert_key, created_ts) VALUES (?, ?)",
            (alert_key, _utc_now_s()),
        )
        conn.commit()

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
