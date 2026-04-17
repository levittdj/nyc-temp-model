# Paper trading fixes — from 6-day performance review (2026-04-15)

## Context

71 paper positions over 6 days. Net P&L: **−$45.70**. The only profitable
signal source is dead bracket sells (+$43.68). BUY_YES is −$54.85 because
the CDF assigns too much mass to brackets far from center — the model
thinks cheap brackets are underpriced but 83% of the time the market was
right. Non-dead SELL_YES is −$34.53 because the model underestimates
brackets it calls overpriced (5 of 12 resolved were the actual winner).

These four changes are PROVISIONAL — they're guardrails against the
known CDF-width miscalibration, not permanent fixes. The real fix is
improving the zone CDF tail shape, which is a separate workstream.

---

## Change 1: Gate BUY_YES on minimum market price

**File:** `intraday_engine.py`

**Why:** BUY_YES on brackets priced <40¢ is 0W/27L (−$71.28). The model's
"huge positive edge" on cheap brackets is its worst signal. The CDF tail
mirroring formula (`L = 2*p10 − p50`) produces systematically too-wide
tails, so the model over-assigns probability to far-from-center brackets.
Until the CDF shape is fixed, these signals are anti-correlated with
reality.

**Add constant** (near the existing PROVISIONAL constants block, around line 37):

```python
BUY_YES_MIN_MARKET_PRICE = 0.40  # PROVISIONAL — don't buy brackets market prices below 40¢;
                                  # model CDF tail-width miscalibration causes false positive
                                  # edge on cheap brackets (0W/27L in first 6 days)
```

**Add guard** in `generate_signals()`, right after the entry signal block
that sets `signal_type = "BUY_YES"` (around where `edge > ENTRY_EDGE_THRESHOLD`
is checked). Replace the existing entry signal logic:

```python
        # 1. Entry signals (time-gated)
        if within_entry_gate:
            if edge > ENTRY_EDGE_THRESHOLD:  # PROVISIONAL
                if market_price >= BUY_YES_MIN_MARKET_PRICE:  # PROVISIONAL — CDF tail guard
                    signal_type = "BUY_YES"
                    reason = f"model {model_prob:.0%} > mkt {market_price:.0%}, edge {edge:+.1%}"
                else:
                    signal_type = "BUY_YES"
                    reason = f"blocked: market price {market_price:.0%} < {BUY_YES_MIN_MARKET_PRICE:.0%} floor"
                    contracts_suggested = 0
                    _sizing_from_guard = True
            elif edge < -ENTRY_EDGE_THRESHOLD:  # PROVISIONAL
                signal_type = "SELL_YES"
                reason = f"model {model_prob:.0%} < mkt {market_price:.0%}, edge {edge:+.1%}"
```

This logs the blocked signal to `intraday_signals` (with `executed=0` and
`contracts_suggested=0`) so we can measure what we would have done without
the guard — crucial for knowing when to remove it.

---

## Change 2: Raise dead bracket floor from 2¢ to 4¢

**File:** `intraday_engine.py`

**Why:** At 2¢ market price and 50 contracts, the taker fee on a dead
bracket sell is ~$0.07 entry + $0.01 exit, and the max profit is $1.00
minus fees = ~$0.92. But 5 of 29 dead bracket sells lost money — the ones
near the 2¢ boundary where fee drag exceeds the tiny payout. At 4¢ the
fee/profit ratio is healthier.

**Change the constant:**

```python
DEAD_BRACKET_FLOOR = 0.04       # PROVISIONAL — was 0.02; raised to improve fee/profit ratio
                                 # on dead bracket sells (5/29 losing at 2¢ floor)
```

---

## Change 3: Gate non-dead SELL_YES on maximum market price

**File:** `intraday_engine.py`

**Why:** Non-dead SELL_YES on brackets priced ≥55¢ lost −$53.73 in 6 days.
These are the model saying "the likely winner is overpriced" — and being
wrong 42% of the time. The model's calibration is worst on the brackets
nearest the outcome (where the CDF underestimates mass). Selling the
high-probability bracket is high-risk when calibration is uncertain.

**Add constant:**

```python
SELL_YES_MAX_MARKET_PRICE = 0.55  # PROVISIONAL — don't sell (buy NO on) brackets
                                   # market prices above 55¢; model underestimates
                                   # probability of likely winners (5/12 hit in first 6 days)
```

**Add guard** in `generate_signals()`, in the same entry signal block,
after the SELL_YES assignment:

```python
            elif edge < -ENTRY_EDGE_THRESHOLD:  # PROVISIONAL
                if market_price <= SELL_YES_MAX_MARKET_PRICE:  # PROVISIONAL — calibration guard
                    signal_type = "SELL_YES"
                    reason = f"model {model_prob:.0%} < mkt {market_price:.0%}, edge {edge:+.1%}"
                else:
                    signal_type = "SELL_YES"
                    reason = f"blocked: market price {market_price:.0%} > {SELL_YES_MAX_MARKET_PRICE:.0%} ceiling"
                    contracts_suggested = 0
                    _sizing_from_guard = True
```

Same pattern — logs the blocked signal for counterfactual analysis.

---

## Change 4: Cap contracts on re-entry to same bracket within a day

**File:** `intraday_engine.py`

**Why:** 15 bracket-day pairs had multiple entries, several compounding
losses (e.g. 65-66 BUY_YES on 4/11: 3 entries, −$12.58 combined).
The cooldown guard exists but is only 60 minutes — multiple entries at
different collector ticks pile into the same losing position.

**Add constant:**

```python
MAX_DAILY_ENTRIES_PER_BRACKET = 1  # PROVISIONAL — one entry per (event_date, bracket_label, side)
                                    # per day; prevents compounding into losing positions
                                    # (15 duplicate bracket-days observed in first 6 days)
```

**Add guard** in `generate_signals()`, after the cooldown guard block
(section 2b), before the EXIT override (section 3):

```python
        # 2c. Guard — daily entry cap: at most MAX_DAILY_ENTRIES_PER_BRACKET entries
        #     per (event_date, bracket_label, side) per day.  PROVISIONAL.
        if signal_type in ("BUY_YES", "SELL_YES") and not _sizing_from_guard:
            side_check = "YES" if signal_type == "BUY_YES" else "NO"
            try:
                conn = sqlite3.connect(str(db_path))
                try:
                    _ensure_intraday_schema(conn)
                    prior_entries = conn.execute(
                        """
                        SELECT COUNT(*)
                        FROM paper_positions
                        WHERE event_date = ? AND bracket_label = ? AND side = ?
                        """,
                        (event_s, label, side_check),
                    ).fetchone()
                    if prior_entries and int(prior_entries[0]) >= MAX_DAILY_ENTRIES_PER_BRACKET:
                        reason = f"blocked: daily entry cap ({prior_entries[0]} prior entries for {label} {side_check})"
                        contracts_suggested = 0
                        _sizing_from_guard = True
                finally:
                    conn.close()
            except Exception:
                pass
```

---

## What these changes would have done (estimated)

Back-of-envelope on the 6-day sample:

| Change | Positions removed | P&L removed | Net effect |
|--------|------------------|-------------|------------|
| BUY_YES floor ≥40¢ | 27 of 28 BUY_YES | −$71.28 removed | +$71.28 |
| Dead bracket floor 4¢ | ~3 marginal sells | ~−$1.50 removed | +$1.50 |
| SELL_YES ceiling ≤55¢ | ~6 non-dead sells | ~−$53.73 removed | +$53.73 |
| 1-entry-per-bracket cap | ~15 re-entries | ~−$20 removed | ~+$20 |

**Conservative estimate: net P&L would have been roughly +$30 to +$50
instead of −$45.70.** Almost entirely from the dead bracket sells, which
is the only signal that doesn't depend on the CDF being well-calibrated.

---

## What these changes do NOT fix

The root cause is the CDF tail shape. `build_zones()` with mirror-reflected
tails (`L = 2*p10 - p50`, `U = 2*p90 - p50`) produces systematically fat
tails. The guardrails above prevent trading on that miscalibration, but
the model's probability estimates are still wrong.

**Next investigation (separate from these changes):**

1. Run the `market_as_probability_brier` Datasette query. If market Brier
   < model Brier on morning rows, we're at **Layer 1 failure** — the
   probability model itself needs fixing before testing for mispricing.

2. Compare `model_prob` vs `model_prob_triplet_cdf` on settled rows to see
   whether the 5-knot CDF helped or hurt (the quint might be making tails
   even wider).

3. Consider replacing mirror-reflected tails with a parametric tail model
   (e.g. fit a skew-normal or use the NBM SD column `TXNSD` to set tail
   width empirically rather than geometrically).
