#!/usr/bin/env python3
"""
intraday_engine.py — paper signal generation and position management.

Implements §3 and §4 of intraday_model_strategy.md.

Three public functions:
  generate_signals()        — build signal list from current bracket rows
  execute_paper_trades()    — persist signals, create/close paper positions
  settle_paper_positions()  — called from backfill_outcome to close open positions

No live order execution.  Paper/simulation only.  All thresholds PROVISIONAL.
"""

from __future__ import annotations

import math
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

# ---------------------------------------------------------------------------
# Constants — all PROVISIONAL; calibrate from observed data after 20+ days
# ---------------------------------------------------------------------------
ENTRY_EDGE_THRESHOLD = 0.08     # PROVISIONAL — min edge to enter (8¢)
EXIT_EDGE_THRESHOLD = 0.02      # PROVISIONAL — exit when edge collapses to <2¢
DEAD_BRACKET_FLOOR = 0.02       # PROVISIONAL — skip dead-bracket sells priced at ≤2¢
BANKROLL = 1000.0               # PROVISIONAL — paper bankroll in dollars
KELLY_FRACTION = 0.25           # PROVISIONAL — quarter Kelly (conservative)
MAX_POSITION_FRAC = 0.15        # PROVISIONAL — max 15% of bankroll per bracket
MAX_CONTRACTS_PER_BRACKET = 50  # PROVISIONAL — hard cap per (event_date, bracket, side)
TAKER_FEE_RATE = 0.07           # PROVISIONAL
MAKER_FEE_RATE = 0.0175         # PROVISIONAL
ENTRY_GATE_START_H = 8          # PROVISIONAL — no new entries before 8am ET
ENTRY_GATE_END_H = 17           # PROVISIONAL — no new entries after 5pm ET
COOLDOWN_MINUTES = 60           # PROVISIONAL — minutes to suppress re-entry after exit

_TZ = "America/New_York"

# ---------------------------------------------------------------------------
# Minimal local schema for the two new tables (also in logger.py SCHEMA for
# full-DB initialization; duplicated here so this module is self-contained).
# ---------------------------------------------------------------------------
_INTRADAY_SCHEMA = """
CREATE TABLE IF NOT EXISTS intraday_signals (
    signal_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_date TEXT NOT NULL,
    snapshot_ts TEXT NOT NULL,
    bracket_label TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    reason TEXT,
    model_prob REAL,
    market_price REAL,
    market_bid REAL,
    market_ask REAL,
    edge REAL,
    observed_max_f REAL,
    hrrr_shift_f REAL,
    trajectory_deviation_f REAL,
    ensemble_ratio REAL,
    forecast_lead_hours REAL,
    kelly_full REAL,
    kelly_fraction REAL,
    contracts_suggested INTEGER,
    price_target REAL,
    executed INTEGER DEFAULT 0,
    execution_ts TEXT,
    execution_price REAL,
    UNIQUE(event_date, snapshot_ts, bracket_label, signal_type)
);

CREATE TABLE IF NOT EXISTS paper_positions (
    position_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_date TEXT NOT NULL,
    bracket_label TEXT NOT NULL,
    side TEXT NOT NULL,
    contracts INTEGER NOT NULL,
    avg_entry_price REAL NOT NULL,
    entry_ts TEXT NOT NULL,
    entry_signal_id INTEGER REFERENCES intraday_signals(signal_id),
    entry_fee REAL,
    status TEXT NOT NULL DEFAULT 'open',
    exit_price REAL,
    exit_ts TEXT,
    exit_signal_id INTEGER REFERENCES intraday_signals(signal_id),
    exit_fee REAL,
    settlement_outcome INTEGER,
    pnl_gross REAL,
    pnl_net REAL,
    UNIQUE(event_date, bracket_label, side, entry_ts)
);
"""


def _ensure_intraday_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(_INTRADAY_SCHEMA)


def _utc_z(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _et_hour(ts: datetime) -> float:
    """Decimal local hour in America/New_York."""
    z = ZoneInfo(_TZ)
    local = ts.astimezone(z) if ts.tzinfo else ts.replace(tzinfo=timezone.utc).astimezone(z)
    return local.hour + local.minute / 60.0


def estimate_fee(contracts: int, yes_price: float, is_maker: bool = False) -> float:
    """Kalshi fee: ceil(rate × contracts × p × (1-p)) to nearest cent."""
    rate = MAKER_FEE_RATE if is_maker else TAKER_FEE_RATE
    return math.ceil(rate * int(contracts) * float(yes_price) * (1.0 - float(yes_price)) * 100) / 100.0


def _kelly_sizing(
    signal_type: str,
    model_prob: float,
    market_bid: Optional[float],
    market_ask: Optional[float],
    market_price: float,
) -> tuple[Optional[float], float, int, float]:
    """
    Compute (kelly_full, kelly_capped, contracts, price_target).

    BUY_YES  — price_target = ask; b = (1-ask)/ask; p = model_prob.
    SELL_YES — price_target = bid; b = bid/(1-bid); p = 1-model_prob (prob of NO).

    kelly_full < 0 → no bet (kelly_capped = 0, contracts = 0).
    All sizing parameters PROVISIONAL.
    """
    ask = float(market_ask) if market_ask is not None else float(market_price)
    bid = float(market_bid) if market_bid is not None else float(market_price)

    if signal_type == "BUY_YES":
        price_target = ask
        if ask <= 0.0 or ask >= 1.0:
            return None, 0.0, 0, price_target
        p = float(model_prob)
        b = (1.0 - ask) / ask
    elif signal_type == "SELL_YES":
        price_target = bid
        if bid <= 0.0 or bid >= 1.0:
            return None, 0.0, 0, price_target
        p = 1.0 - float(model_prob)  # prob of NO winning
        b = bid / (1.0 - bid)
    else:
        return None, 0.0, 0, float(market_price)

    if b <= 0.0:
        return 0.0, 0.0, 0, price_target

    kelly_full = (p * b - (1.0 - p)) / b
    kelly_capped = max(0.0, min(kelly_full * KELLY_FRACTION, MAX_POSITION_FRAC))  # PROVISIONAL
    contracts = int(math.floor(kelly_capped * BANKROLL / price_target)) if price_target > 0 else 0
    contracts = min(contracts, MAX_CONTRACTS_PER_BRACKET)  # PROVISIONAL
    return kelly_full, kelly_capped, contracts, price_target


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_signals(
    event_date: date,
    rows: list,
    db_path: Path,
    snapshot_ts: datetime,
    observed_max_f: Optional[float],
    *,
    hrrr_shift_f: Optional[float] = None,
    trajectory_deviation_f: Optional[float] = None,
    ensemble_ratio: Optional[float] = None,
    forecast_lead_hours: Optional[float] = None,
) -> list[dict]:
    """
    Generate entry, exit, and dead-bracket signals for today's brackets.

    Signal priority (later overrides earlier per §3.1):
      1. Entry: BUY_YES if edge > ENTRY_EDGE_THRESHOLD, SELL_YES if edge < -threshold.
      2. Dead-bracket: SELL_YES when model_prob==0 and market_price > DEAD_BRACKET_FLOOR.
         Exempt from time gates.  Reason encodes cause:
           "dead bracket (truncation: ...)" — observed max exceeded bracket upper_f.
             Physical constraint; cannot lose.  High-conviction signal.
           "dead bracket (model: CDF tail below bracket)" — HRRR/ensemble shift pushed
             the tail below the bracket boundary.  Still a forecast; can be wrong.
         The §5.3 decomposition query distinguishes these via reason LIKE patterns.
      2a. Guard — opposing position: if BUY_YES and open NO position exists on the
          same bracket, replace with EXIT on the NO position (and vice versa).
          reason = "exit: opposing signal (was {side}, new signal {stype})".
          This guard has priority over the cooldown guard.
      2b. Guard — cooldown: if BUY_YES/SELL_YES and this bracket had a position
          exit within the last COOLDOWN_MINUTES (PROVISIONAL: 60 min), skip the
          entry.  Signal is still appended with contracts_suggested=0 and
          reason="blocked: cooldown (exited ...)" so it lands in intraday_signals
          with executed=0 for post-hoc analysis.
      3. Exit: overrides everything when existing open position and |edge| < EXIT_EDGE_THRESHOLD.

    Time gates (PROVISIONAL):
      - No entries before 8am or after 5pm ET.
      - Dead-bracket sells are exempt (physical constraint, not forecast).

    Returns list of signal dicts matching intraday_signals schema.
    Signals are not yet written to DB — call execute_paper_trades() to persist.
    """
    if snapshot_ts.tzinfo is None:
        snapshot_ts = snapshot_ts.replace(tzinfo=timezone.utc)

    et_hour = _et_hour(snapshot_ts)
    within_entry_gate = ENTRY_GATE_START_H <= et_hour < ENTRY_GATE_END_H  # PROVISIONAL
    snap_s = _utc_z(snapshot_ts)
    event_s = event_date.isoformat()

    # Load open positions to enable EXIT signals and opposing-position guard
    open_positions: dict[tuple[str, str], dict[str, Any]] = {}
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            _ensure_intraday_schema(conn)
            for bl, side, ctrs, aep in conn.execute(
                """
                SELECT bracket_label, side, contracts, avg_entry_price
                FROM paper_positions
                WHERE event_date = ? AND status = 'open'
                """,
                (event_s,),
            ).fetchall():
                open_positions[(str(bl), str(side))] = {
                    "contracts": ctrs,
                    "avg_entry_price": aep,
                }
        finally:
            conn.close()
    except Exception:
        pass

    # Load recent exits for cooldown guard (COOLDOWN_MINUTES PROVISIONAL)
    recent_exit_times: dict[str, str] = {}  # bracket_label → latest exit_ts string
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            _ensure_intraday_schema(conn)
            cutoff_s = _utc_z(snapshot_ts - timedelta(minutes=COOLDOWN_MINUTES))
            for bl, last_exit in conn.execute(
                """
                SELECT bracket_label, MAX(exit_ts) AS last_exit
                FROM paper_positions
                WHERE event_date = ? AND status = 'exited' AND exit_ts >= ?
                GROUP BY bracket_label
                """,
                (event_s, cutoff_s),
            ).fetchall():
                recent_exit_times[str(bl)] = str(last_exit)
        finally:
            conn.close()
    except Exception:
        pass

    signals: list[dict] = []

    for r in rows:
        label = getattr(r, "bracket_label", None)
        if label is None:
            continue

        model_prob = float(r.model_prob)
        market_price = float(r.market_price)
        edge = model_prob - market_price
        bid_raw = getattr(r, "market_bid", None)
        ask_raw = getattr(r, "market_ask", None)
        bid = float(bid_raw) if bid_raw is not None else market_price
        ask = float(ask_raw) if ask_raw is not None else market_price
        mid_price = (bid + ask) / 2.0

        signal_type: Optional[str] = None
        reason: Optional[str] = None

        # 1. Entry signals (time-gated)
        if within_entry_gate:
            if edge > ENTRY_EDGE_THRESHOLD:  # PROVISIONAL
                signal_type = "BUY_YES"
                reason = f"model {model_prob:.0%} > mkt {market_price:.0%}, edge {edge:+.1%}"
            elif edge < -ENTRY_EDGE_THRESHOLD:  # PROVISIONAL
                signal_type = "SELL_YES"
                reason = f"model {model_prob:.0%} < mkt {market_price:.0%}, edge {edge:+.1%}"

        # 2. Dead-bracket (overrides entry; exempt from time gate)
        # Split reason by cause so §5.3 decomposition can separate win rates:
        #   truncation  — observed max already exceeded bracket; physically impossible.
        #   model       — CDF shift/narrowing pushed tail past bracket boundary; still a forecast.
        if model_prob == 0.0 and market_price > DEAD_BRACKET_FLOOR:  # PROVISIONAL
            signal_type = "SELL_YES"
            upper_f = getattr(r, "upper_f", None)
            is_truncation = (
                observed_max_f is not None
                and upper_f is not None
                and float(upper_f) <= float(observed_max_f) + 0.5  # continuity convention
            )
            if is_truncation:
                reason = (
                    f"dead bracket (truncation: observed {observed_max_f:.0f}F), "
                    f"still priced {market_price:.0%}"
                )
            else:
                reason = (
                    f"dead bracket (model: CDF tail below bracket), "
                    f"still priced {market_price:.0%}"
                )

        # 2a. Guard — opposing position: if BUY_YES and open NO exists, EXIT the NO instead
        #     (and vice versa).  Priority over cooldown guard.
        _sizing_from_guard = False
        contracts_suggested: int = 0
        price_target: float = mid_price
        existing_yes = open_positions.get((label, "YES"))
        existing_no = open_positions.get((label, "NO"))
        if signal_type in ("BUY_YES", "SELL_YES"):
            opp_side = "NO" if signal_type == "BUY_YES" else "YES"
            opp_pos = open_positions.get((label, opp_side))
            if opp_pos is not None:
                old_stype = signal_type
                signal_type = "EXIT"
                reason = f"exit: opposing signal (was {opp_side}, new signal {old_stype})"
                contracts_suggested = int(opp_pos["contracts"])
                price_target = mid_price
                _sizing_from_guard = True

        # 2b. Guard — cooldown: suppress re-entry within COOLDOWN_MINUTES of a prior exit
        if signal_type in ("BUY_YES", "SELL_YES") and not _sizing_from_guard:
            if label in recent_exit_times:
                reason = f"blocked: cooldown (exited {recent_exit_times[label]})"
                contracts_suggested = 0  # logged to DB with executed=0 but no position opened
                _sizing_from_guard = True

        # 3. Exit: overrides everything when edge collapsed on an open same-side position
        if abs(edge) < EXIT_EDGE_THRESHOLD:  # PROVISIONAL
            if existing_yes is not None:
                signal_type = "EXIT"
                reason = f"edge collapsed to {edge:+.1%} (YES position)"
                _sizing_from_guard = False  # recompute sizing below
            elif existing_no is not None:
                signal_type = "EXIT"
                reason = f"edge collapsed to {edge:+.1%} (NO position)"
                _sizing_from_guard = False

        if signal_type is None:
            continue

        # Kelly sizing — skipped for guard-resolved signals (opposing EXIT, cooldown)
        kelly_full: Optional[float] = None
        kelly_capped_val: float = 0.0
        if not _sizing_from_guard:
            contracts_suggested = 0
            price_target = mid_price
            if signal_type in ("BUY_YES", "SELL_YES"):
                kelly_full, kelly_capped_val, contracts_suggested, price_target = _kelly_sizing(
                    signal_type, model_prob, bid_raw, ask_raw, market_price
                )
            elif signal_type == "EXIT":
                existing = existing_yes or existing_no
                contracts_suggested = int(existing["contracts"]) if existing else 0
                price_target = mid_price

        signals.append({
            "event_date": event_s,
            "snapshot_ts": snap_s,
            "bracket_label": label,
            "signal_type": signal_type,
            "reason": reason,
            "model_prob": model_prob,
            "market_price": market_price,
            "market_bid": bid,
            "market_ask": ask,
            "edge": edge,
            "observed_max_f": observed_max_f,
            "hrrr_shift_f": hrrr_shift_f,
            "trajectory_deviation_f": trajectory_deviation_f,
            "ensemble_ratio": ensemble_ratio,
            "forecast_lead_hours": forecast_lead_hours,
            "kelly_full": kelly_full,
            "kelly_fraction": KELLY_FRACTION if kelly_full is not None else None,
            "contracts_suggested": contracts_suggested,
            "price_target": price_target,
        })

    return signals


def execute_paper_trades(signals: list[dict], db_path: Path) -> int:
    """
    Persist signals to intraday_signals (exhaustive log; executed=0 by default),
    then execute eligible ones as paper positions.

    Position rules (§3.4):
      - BUY_YES: open YES position unless bracket already at MAX_CONTRACTS.
      - SELL_YES: open NO position (equivalent to buying NO) unless maxed out.
      - EXIT: close the open position at mid-price; compute interim P&L.
      - One open position per (event_date, bracket_label, side) at a time.

    avg_entry_price stored as:
      YES: ask price (what we paid to go long YES).
      NO : (1 - bid) — the NO contract cost (bid = YES price we sold).

    Returns count of positions opened or closed.
    """
    if not signals:
        return 0

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    positions_changed = 0

    try:
        _ensure_intraday_schema(conn)

        for sig in signals:
            # Insert signal (log everything, including those not executed)
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO intraday_signals (
                    event_date, snapshot_ts, bracket_label,
                    signal_type, reason,
                    model_prob, market_price, market_bid, market_ask, edge,
                    observed_max_f, hrrr_shift_f, trajectory_deviation_f,
                    ensemble_ratio, forecast_lead_hours,
                    kelly_full, kelly_fraction, contracts_suggested, price_target,
                    executed, execution_ts, execution_price
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,NULL,NULL)
                """,
                (
                    sig["event_date"], sig["snapshot_ts"], sig["bracket_label"],
                    sig["signal_type"], sig["reason"],
                    sig["model_prob"], sig["market_price"], sig["market_bid"], sig["market_ask"], sig["edge"],
                    sig["observed_max_f"], sig["hrrr_shift_f"], sig["trajectory_deviation_f"],
                    sig["ensemble_ratio"], sig["forecast_lead_hours"],
                    sig["kelly_full"], sig["kelly_fraction"], sig["contracts_suggested"], sig["price_target"],
                ),
            )
            signal_id = cursor.lastrowid
            # If INSERT was ignored (duplicate), fetch the existing signal_id
            if cursor.rowcount == 0:
                row = conn.execute(
                    """
                    SELECT signal_id FROM intraday_signals
                    WHERE event_date=? AND snapshot_ts=? AND bracket_label=? AND signal_type=?
                    """,
                    (sig["event_date"], sig["snapshot_ts"], sig["bracket_label"], sig["signal_type"]),
                ).fetchone()
                if row:
                    signal_id = int(row[0])
                else:
                    continue
            else:
                signal_id = int(cursor.lastrowid)

            exec_ts = _utc_z(datetime.now(timezone.utc))
            stype = sig["signal_type"]
            label = sig["bracket_label"]
            event_s = sig["event_date"]
            contracts = int(sig.get("contracts_suggested") or 0)

            if stype == "EXIT":
                # Close the first open position found (YES then NO)
                for side in ("YES", "NO"):
                    pos = conn.execute(
                        """
                        SELECT position_id, contracts, avg_entry_price, entry_fee
                        FROM paper_positions
                        WHERE event_date=? AND bracket_label=? AND side=? AND status='open'
                        ORDER BY entry_ts ASC LIMIT 1
                        """,
                        (event_s, label, side),
                    ).fetchone()
                    if pos:
                        pid, n_ctrs, aep, e_fee = pos
                        exit_price = float(sig["price_target"])  # mid-price
                        exit_fee = estimate_fee(int(n_ctrs), exit_price)
                        n = int(n_ctrs)
                        aep_f = float(aep)
                        if side == "YES":
                            pnl_gross = n * (exit_price - aep_f)
                        else:
                            # NO: avg_entry_price is the NO contract cost (1-bid).
                            # Exiting NO = selling NO at (1 - exit_price).
                            pnl_gross = n * ((1.0 - exit_price) - aep_f)
                        pnl_net = pnl_gross - float(e_fee or 0.0) - exit_fee
                        conn.execute(
                            """
                            UPDATE paper_positions
                            SET status='exited', exit_price=?, exit_ts=?,
                                exit_signal_id=?, exit_fee=?, pnl_gross=?, pnl_net=?
                            WHERE position_id=?
                            """,
                            (exit_price, exec_ts, signal_id, exit_fee, pnl_gross, pnl_net, int(pid)),
                        )
                        conn.execute(
                            """
                            UPDATE intraday_signals
                            SET executed=1, execution_ts=?, execution_price=?
                            WHERE signal_id=?
                            """,
                            (exec_ts, exit_price, signal_id),
                        )
                        positions_changed += 1
                        break  # one position closed per EXIT signal

            elif stype in ("BUY_YES", "SELL_YES"):
                if contracts <= 0:
                    continue

                side = "YES" if stype == "BUY_YES" else "NO"
                if stype == "BUY_YES":
                    avg_entry_price = float(sig["market_ask"] if sig["market_ask"] else sig["market_price"])
                    yes_price_for_fee = avg_entry_price
                else:
                    # SELL_YES → long NO at (1 - bid)
                    bid_p = float(sig["market_bid"] if sig["market_bid"] else sig["market_price"])
                    avg_entry_price = 1.0 - bid_p
                    yes_price_for_fee = bid_p

                # Check existing open contracts for this side
                existing = conn.execute(
                    """
                    SELECT COALESCE(SUM(contracts), 0)
                    FROM paper_positions
                    WHERE event_date=? AND bracket_label=? AND side=? AND status='open'
                    """,
                    (event_s, label, side),
                ).fetchone()
                current_contracts = int(existing[0]) if existing else 0
                if current_contracts >= MAX_CONTRACTS_PER_BRACKET:  # PROVISIONAL
                    continue

                contracts = min(contracts, MAX_CONTRACTS_PER_BRACKET - current_contracts)
                if contracts <= 0:
                    continue

                entry_fee = estimate_fee(contracts, yes_price_for_fee)
                conn.execute(
                    """
                    INSERT OR IGNORE INTO paper_positions (
                        event_date, bracket_label, side,
                        contracts, avg_entry_price, entry_ts,
                        entry_signal_id, entry_fee, status
                    ) VALUES (?,?,?,?,?,?,?,?,'open')
                    """,
                    (event_s, label, side, contracts, avg_entry_price, exec_ts, signal_id, entry_fee),
                )
                if conn.execute("SELECT changes()").fetchone()[0] > 0:
                    conn.execute(
                        """
                        UPDATE intraday_signals
                        SET executed=1, execution_ts=?, execution_price=?
                        WHERE signal_id=?
                        """,
                        (exec_ts, sig["price_target"], signal_id),
                    )
                    positions_changed += 1

        conn.commit()
    finally:
        conn.close()

    return positions_changed


def settle_paper_positions(db_path: Path, event_date: date, actual_max_f: float) -> int:
    """
    Settle all open paper_positions for event_date using actual_max_f.

    Called from logger.backfill_outcome after updating bracket outcomes.
    Also stamps settlement_outcome on already-exited positions (no P&L change).

    P&L formula (§3.5):
      YES side: pnl_gross = contracts × (outcome − avg_entry_price)
      NO  side: pnl_gross = contracts × ((1 − outcome) − avg_entry_price)
        where avg_entry_price for NO = (1 − YES_bid) at entry.

    pnl_net = pnl_gross − entry_fee  (no exit fee at settlement)

    Returns count of positions settled (open→settled; exited positions not counted).
    """
    from morning_model import kalshi_settlement_wins  # local import avoids circularity

    event_s = event_date.isoformat()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    settled = 0

    try:
        _ensure_intraday_schema(conn)

        # Compute outcome per distinct bracket_label for this event_date
        bracket_outcomes: dict[str, int] = {}
        for (lbl,) in conn.execute(
            "SELECT DISTINCT bracket_label FROM paper_positions WHERE event_date=?",
            (event_s,),
        ).fetchall():
            try:
                bracket_outcomes[str(lbl)] = 1 if kalshi_settlement_wins(str(lbl), float(actual_max_f)) else 0
            except Exception:
                pass

        if not bracket_outcomes:
            return 0

        # Settle open positions with full P&L
        for pid, lbl, side, n_ctrs, aep, e_fee in conn.execute(
            """
            SELECT position_id, bracket_label, side, contracts, avg_entry_price, entry_fee
            FROM paper_positions
            WHERE event_date = ? AND status = 'open'
            """,
            (event_s,),
        ).fetchall():
            outcome = bracket_outcomes.get(str(lbl))
            if outcome is None:
                continue
            n = int(n_ctrs)
            aep_f = float(aep)
            if side == "YES":
                pnl_gross = n * (float(outcome) - aep_f)
            else:  # NO: avg_entry_price = (1 - YES_bid at entry)
                no_payout = 1.0 if outcome == 0 else 0.0
                pnl_gross = n * (no_payout - aep_f)
            pnl_net = pnl_gross - float(e_fee or 0.0)
            conn.execute(
                """
                UPDATE paper_positions
                SET status='settled', settlement_outcome=?, pnl_gross=?, pnl_net=?
                WHERE position_id=?
                """,
                (outcome, pnl_gross, pnl_net, int(pid)),
            )
            settled += 1

        # Stamp settlement_outcome on exited positions (P&L already set at exit)
        for pid, lbl in conn.execute(
            """
            SELECT position_id, bracket_label
            FROM paper_positions
            WHERE event_date = ? AND status = 'exited' AND settlement_outcome IS NULL
            """,
            (event_s,),
        ).fetchall():
            outcome = bracket_outcomes.get(str(lbl))
            if outcome is not None:
                conn.execute(
                    "UPDATE paper_positions SET settlement_outcome=? WHERE position_id=?",
                    (outcome, int(pid)),
                )

        conn.commit()
    finally:
        conn.close()

    return settled
