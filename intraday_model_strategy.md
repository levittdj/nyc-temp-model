# Intraday Model Strategy: Paper Execution Engine for KXHIGHNY

**Date:** 2026-04-10
**Phase:** V0 extension — parallel evaluation track alongside morning gate
**Status:** Strategy document → implementation spec

---

## 1. What we're building and why

The morning model captures a single 7am snapshot: NBM percentiles → piecewise CDF → bracket probabilities → edge vs market. That snapshot is frozen for evaluation purposes. But the actual daily high emerges over ~12 hours of observable weather, and Kalshi prices reprice continuously as bots and humans incorporate new data.

The intraday model does three things the morning model cannot:

1. **Updates probability estimates** as observations constrain the distribution (truncation, trajectory deviation, HRRR shifts, ensemble convergence)
2. **Generates explicit trade signals** — bracket, direction (buy/sell YES), price, and sizing — logged to a new DB table
3. **Tracks simulated P&L** with position management, fee modeling, and settlement resolution

This runs as a parallel evaluation track. The morning v0 gate (Brier, permutation test, paper P&L on `snapshot_type='morning'` rows) remains unchanged and untouched. The intraday engine gets its own evaluation metrics computed from its own rows.

---

## 2. Four signals that change `model_prob`

The collector already logs ensemble spread, HRRR max, observed max, and METAR data. Currently only truncation and HRRR shift actually modify `model_prob` on intraday rows. This section specifies how all four signals should work together, including the order of operations.

### 2.1 Signal A: Observed-max truncation (already implemented)

**What it does:** Once KNYC METAR has recorded X°F as the running high, brackets with `upper_f ≤ X + 0.5` are dead — probability zero, renormalize the rest.

**Current implementation:** `truncate_and_renormalize()` in `morning_model.py`, called by collector for today's market only.

**Assessment:** This is the single highest-confidence intraday signal. It's not a forecast — it's a physical constraint. The temperature has *already been observed*; any bracket entirely below it is mathematically impossible. Markets that still price these brackets above 1–2¢ are giving away free money.

**Change needed:** None to the math. But the paper execution engine should flag dead-bracket sell signals automatically (see §3).

### 2.2 Signal B: HRRR CDF shift (already implemented)

**What it does:** HRRR runs hourly at 3km resolution. When its implied daily max diverges from NBM p50 by ≥1°F, shift all CDF knots by half the disagreement.

**Current implementation:** `apply_hrrr_shift()` in `morning_model.py`, called by collector. Threshold 1°F and blend weight 0.5 are both PROVISIONAL.

**Assessment:** HRRR is deterministic (no native ensemble), so it provides a point estimate, not a distribution. The half-weight blend is conservative but unprincipled. The key question is whether HRRR adds information beyond what NBM already incorporates — NBM *includes* HRRR in its blend, but with multi-model averaging that dilutes HRRR's short-range advantage. At lead times under 6 hours, HRRR should dominate NBM because it assimilates the most recent radar/surface observations.

**Changes needed:**

- **Lead-time-dependent blend weight (PROVISIONAL):** At forecast_lead_hours > 12, NBM is king and HRRR weight should be near zero. At lead < 6h, HRRR weight should increase to 0.6–0.7. Proposed schedule:

  | forecast_lead_hours | hrrr_blend_weight |
  |---|---|
  | > 12 | 0.0 (no shift) |
  | 6–12 | 0.3 |
  | 3–6 | 0.5 |
  | < 3 | 0.7 |

  All values PROVISIONAL. Log the weight used per snapshot for later calibration.

- **Direction check:** If HRRR and the METAR trajectory agree (both warmer or both cooler than NBM), increase confidence. If they disagree, reduce the shift. This is Signal D interaction — see §2.4.

### 2.3 Signal C: Ensemble spread as CDF width modulator (new)

**What it does:** When GEFS (31-member) and ECMWF (51-member) ensembles show wider spread than NBM p10–p90 implies, the model's tails are too narrow — widen them. When ensembles are tighter, tails are too wide — narrow them.

**Currently:** Ensemble spread is logged (`ens_gefs_spread_f`, `ens_ecmwf_spread_f`) but does not modify `model_prob`.

**Design:**

The NBM p10–p90 range defines the model's "80% confidence interval" (CDF from 0.10 to 0.90). The ensemble spread (max–min across members) covers a wider range but with different semantics — it's not calibrated to specific quantiles. The ensemble standard deviation is more directly comparable.

**Approach — tail scaling factor:**

```
combined_ens_sd = weighted_mean(ens_gefs_sd_f, ens_ecmwf_sd_f)
    where weights = (31, 51) normalized  # member counts
nbm_implied_sd = nbm_spread_raw / 2.56  # p10-p90 ≈ 2.56σ for normal
ratio = combined_ens_sd / nbm_implied_sd

if ratio > 1.15:   # PROVISIONAL — ensembles say wider
    scale tails by ratio (widen L and U)
elif ratio < 0.85: # PROVISIONAL — ensembles say narrower
    scale tails by ratio (narrow L and U)
else:
    no change
```

**Implementation:** New function `apply_ensemble_width()` that takes the ZoneModel and ratio, returns a modified ZoneModel with adjusted L and U (and optionally p10/p90). The inner knots (p25, p50, p75) stay fixed — ensemble spread affects *tails*, not the central forecast.

**Risk:** Double-counting. NBM already incorporates GFS and ECMWF in its blend. The ensemble spread signal is most useful when it *diverges* from NBM's implied spread, which happens when:
- A recent model run has shifted dramatically but NBM hasn't updated yet
- The ensemble distribution is non-Gaussian (bimodal — e.g., frontal passage timing uncertainty)

Log the ratio and the adjustment applied. Compare adjusted vs unadjusted Brier scores after 20+ days.

### 2.4 Signal D: METAR trajectory deviation (new)

**What it does:** Compare the observed temperature trajectory (hourly METAR readings) against the *expected* trajectory for a day with the forecasted high. If morning temperatures are systematically running hot or cold relative to expectation, shift the distribution.

**Currently:** METAR observations are captured at 5-minute resolution in `metar_observations`. The running max feeds truncation. But the *trajectory shape* — how temperatures are evolving hour by hour — is not used.

**Design:**

The diurnal temperature cycle follows an approximately sinusoidal pattern:

```
T_expected(hour) = T_low + (T_high - T_low) × sin_factor(hour)

where:
  T_low = overnight_low_f (from morning snapshot or METAR min since midnight)
  T_high = nbm_p50_adj (bias-corrected forecast high)
  sin_factor(hour) = sin(π × (hour - sunrise) / (2 × (peak_hour - sunrise)))
    for hour in [sunrise, peak_hour]
```

Peak hour is ~14:00 local (PROVISIONAL). Sunrise varies by date (~05:30–07:00 ET in NYC spring).

At each collector tick, compute:

```
trajectory_deviation = mean(T_observed(h) - T_expected(h)) for h in [sunrise, now]
```

If the morning is running 2°F warmer than expected across multiple observations, the daily high will likely also exceed the forecast. Apply a **shift equal to the trajectory deviation, discounted by remaining uncertainty:**

```
shift_f = trajectory_deviation × confidence_factor

where confidence_factor depends on time of day:
  before 10am:  0.3  (morning data is noisy, sea breeze hasn't formed)
  10am–noon:    0.5  (pattern is establishing)
  noon–2pm:     0.7  (peak approaching, less room for reversal)
  after 2pm:    0.9  (high is nearly determined)
```

All values PROVISIONAL.

**Implementation:** New function `compute_trajectory_deviation()` that:
1. Queries `metar_observations` for today's event_date
2. Computes expected trajectory from overnight low + forecast high
3. Returns deviation and confidence factor

New function `apply_trajectory_shift()` that shifts the CDF knots by `deviation × confidence_factor`, similar to HRRR shift but from a different signal source.

**Interaction with HRRR shift:** Both HRRR and trajectory deviation may want to shift the CDF in the same direction. When they agree, the combined shift should be stronger. When they disagree, it's ambiguous — HRRR might be seeing an approaching front that hasn't affected surface temps yet.

**Proposed combination:**

```
if sign(hrrr_shift) == sign(trajectory_shift):
    combined_shift = max(abs(hrrr_shift), abs(trajectory_shift)) × 1.2  # boosted
else:
    combined_shift = (hrrr_shift + trajectory_shift) / 2  # average (muted)
```

PROVISIONAL. Log both individual shifts and the combined shift.

### 2.5 Order of operations

Each collector tick for today's market applies signals in this order:

```
1. Start with NBM CDF (p10/p25/p50/p75/p90 + bias → ZoneModel)
2. Apply ensemble width modulation (Signal C) → adjusted ZoneModel
3. Compute bracket_prob from adjusted ZoneModel
4. Apply HRRR shift (Signal B) → shifted bracket probs
5. Apply trajectory shift (Signal D) → further shifted bracket probs
6. Apply observed-max truncation (Signal A) → final bracket probs
7. Renormalize to sum = 1.0
8. Compute edge = model_prob - market_price
9. Generate trade signals (§3)
```

Truncation is always last because it's a hard physical constraint — no forecast adjustment can bring a dead bracket back. Width modulation is first because it changes the *shape* of the distribution before point shifts are applied.

---

## 3. Paper execution engine

### 3.1 Signal generation

At each collector tick (every 30 minutes), for each bracket of today's market:

```python
signal = None
edge = model_prob - market_price

# Entry signals
if edge > ENTRY_EDGE_THRESHOLD:  # PROVISIONAL: 0.08 (8%)
    signal = "BUY_YES"
    reason = f"model {model_prob:.0%} > mkt {market_price:.0%}, edge {edge:+.1%}"

elif edge < -ENTRY_EDGE_THRESHOLD:
    signal = "SELL_YES"  # equivalently, BUY_NO
    reason = f"model {model_prob:.0%} < mkt {market_price:.0%}, edge {edge:+.1%}"

# Dead bracket signal (truncation)
if model_prob == 0.0 and market_price > 0.02:  # PROVISIONAL: 2¢ floor
    signal = "SELL_YES"
    reason = f"dead bracket (observed max {observed_max_f:.0f}F), still priced {market_price:.0%}"

# Conviction exit: model and market converge
if existing_position and abs(edge) < EXIT_EDGE_THRESHOLD:  # PROVISIONAL: 0.02 (2%)
    signal = "EXIT"
    reason = f"edge collapsed to {edge:+.1%}"
```

**Thresholds:**
- `ENTRY_EDGE_THRESHOLD = 0.08` — 8¢ edge required to enter. This must exceed the round-trip cost (taker fees ~3.5¢ at midpoint + half-spread ~1–2¢ = ~5¢). 8¢ provides ~3¢ margin. PROVISIONAL.
- `EXIT_EDGE_THRESHOLD = 0.02` — exit when edge is within 2¢ of zero. Don't hold to settlement unless edge persists. PROVISIONAL.
- `DEAD_BRACKET_FLOOR = 0.02` — don't sell dead brackets priced at 1–2¢; the fee exceeds the profit. PROVISIONAL.

**Time gates (PROVISIONAL):**
- No new entries before 8:00 AM ET (market may be thin, prices unreliable)
- No new entries after 5:00 PM ET (approaching settlement, not enough time for mean reversion)
- Dead-bracket sells exempt from time gates (the signal is physical, not forecast-based)

### 3.2 Position sizing

Use fractional Kelly criterion:

```python
# Kelly fraction for a binary bet
# p = model_prob, q = 1 - p
# b = payout odds = (1 - price) / price for a YES buy at `price`
#   (you pay `price`, receive 1.00 if correct, net gain = 1 - price)

if signal == "BUY_YES":
    p = model_prob
    b = (1.0 - market_ask) / market_ask  # use ask for buys
    kelly_full = (p * b - (1 - p)) / b
elif signal == "SELL_YES":
    p = 1.0 - model_prob  # prob of NO
    b = market_bid / (1.0 - market_bid)  # payout on NO side
    kelly_full = (p * b - (1 - p)) / b

kelly_fraction = 0.25  # PROVISIONAL — quarter-Kelly
kelly_capped = max(0, min(kelly_full * kelly_fraction, MAX_POSITION_FRAC))

# Convert to contracts
contracts = floor(kelly_capped * BANKROLL / price)
contracts = min(contracts, MAX_CONTRACTS_PER_BRACKET)  # PROVISIONAL: 50
```

**Parameters (all PROVISIONAL):**
- `BANKROLL = 1000` — paper bankroll in dollars
- `kelly_fraction = 0.25` — quarter Kelly (conservative for estimation error)
- `MAX_CONTRACTS_PER_BRACKET = 50` — hard cap per bracket per event_date
- `MAX_POSITION_FRAC = 0.15` — never more than 15% of bankroll in one bracket

### 3.3 Fee model

Kalshi fees follow: `round_up(fee_rate × contracts × price × (1 - price))`

```python
TAKER_FEE_RATE = 0.07
MAKER_FEE_RATE = 0.0175

def estimate_fee(contracts: int, price: float, is_maker: bool = False) -> float:
    rate = MAKER_FEE_RATE if is_maker else TAKER_FEE_RATE
    return math.ceil(rate * contracts * price * (1 - price) * 100) / 100

# For paper trading, assume TAKER unless signal says "limit order"
# Maker orders require patience and may not fill — model as taker for conservatism
```

### 3.4 Position management

Track open positions per (event_date, bracket_label):

```
position = {
    event_date: "2026-04-10",
    bracket_label: "62-63",
    side: "YES",          # or "NO"
    contracts: 25,
    avg_entry_price: 0.42,
    entry_ts: "2026-04-10T14:30:00Z",
    entry_fee: 0.52,
    status: "open",       # open / closed / settled
    exit_price: null,
    exit_ts: null,
    exit_fee: null,
    pnl_gross: null,
    pnl_net: null,        # after fees
    settlement_outcome: null,  # 1 or 0 (from backfill)
}
```

**Rules:**
- Only one open position per (event_date, bracket_label, side) at a time
- If already long YES on 62-63 and a new BUY_YES signal fires, only add if current position < MAX_CONTRACTS and new edge > ENTRY_EDGE_THRESHOLD
- EXIT signals close the full position at current mid-price
- At settlement (from backfill `outcome`), mark position as settled and compute final P&L

### 3.5 Settlement P&L

```python
if side == "YES" and outcome == 1:
    pnl_gross = contracts * (1.00 - avg_entry_price)
elif side == "YES" and outcome == 0:
    pnl_gross = contracts * (0.00 - avg_entry_price)  # total loss
elif side == "NO" and outcome == 0:
    pnl_gross = contracts * (1.00 - avg_entry_price)  # NO paid off
elif side == "NO" and outcome == 1:
    pnl_gross = contracts * (0.00 - avg_entry_price)

pnl_net = pnl_gross - entry_fee - exit_fee
```

If a position was exited before settlement, P&L uses the exit price instead.

---

## 4. Database schema additions

### 4.1 New table: `intraday_signals`

```sql
CREATE TABLE IF NOT EXISTS intraday_signals (
    signal_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_date TEXT NOT NULL,
    snapshot_ts TEXT NOT NULL,
    bracket_label TEXT NOT NULL,

    signal_type TEXT NOT NULL,      -- BUY_YES / SELL_YES / EXIT
    reason TEXT,

    model_prob REAL,
    market_price REAL,
    market_bid REAL,
    market_ask REAL,
    edge REAL,

    -- Signal inputs (provenance)
    observed_max_f REAL,
    hrrr_shift_f REAL,
    trajectory_deviation_f REAL,
    ensemble_ratio REAL,
    forecast_lead_hours REAL,

    -- Sizing
    kelly_full REAL,
    kelly_fraction REAL,
    contracts_suggested INTEGER,
    price_target REAL,              -- bid for sells, ask for buys

    -- Execution tracking (paper)
    executed INTEGER DEFAULT 0,     -- 0=signal only, 1=paper-filled
    execution_ts TEXT,
    execution_price REAL,

    UNIQUE(event_date, snapshot_ts, bracket_label, signal_type)
);
```

### 4.2 New table: `paper_positions`

```sql
CREATE TABLE IF NOT EXISTS paper_positions (
    position_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_date TEXT NOT NULL,
    bracket_label TEXT NOT NULL,
    side TEXT NOT NULL,              -- YES / NO

    contracts INTEGER NOT NULL,
    avg_entry_price REAL NOT NULL,
    entry_ts TEXT NOT NULL,
    entry_signal_id INTEGER REFERENCES intraday_signals(signal_id),
    entry_fee REAL,

    status TEXT NOT NULL DEFAULT 'open',  -- open / exited / settled
    exit_price REAL,
    exit_ts TEXT,
    exit_signal_id INTEGER REFERENCES intraday_signals(signal_id),
    exit_fee REAL,

    settlement_outcome INTEGER,     -- 1 or 0 (backfilled)
    pnl_gross REAL,
    pnl_net REAL,

    UNIQUE(event_date, bracket_label, side, entry_ts)
);
```

### 4.3 New columns on `bracket_snapshots` (intraday rows)

```sql
-- Already present:
--   observed_max_f_at_snapshot, hrrr_max_f, hrrr_shift_applied_f, metar_new_obs

-- New:
ALTER TABLE bracket_snapshots ADD COLUMN trajectory_deviation_f REAL;
ALTER TABLE bracket_snapshots ADD COLUMN trajectory_confidence REAL;
ALTER TABLE bracket_snapshots ADD COLUMN ensemble_width_ratio REAL;
ALTER TABLE bracket_snapshots ADD COLUMN combined_shift_f REAL;
ALTER TABLE bracket_snapshots ADD COLUMN hrrr_blend_weight REAL;
```

---

## 5. Evaluation framework

The intraday model gets its own metrics, computed separately from the morning v0 gate.

### 5.1 Intraday Brier score

Compute per-snapshot Brier score for intraday rows:

```sql
SELECT
    CAST(ROUND(hours_to_settle) AS INTEGER) AS hour_bucket,
    AVG((model_prob - outcome) * (model_prob - outcome)) AS brier_score,
    COUNT(*) AS n
FROM bracket_snapshots
WHERE snapshot_type = 'intraday'
  AND outcome IS NOT NULL
GROUP BY hour_bucket
ORDER BY hour_bucket
```

**Compare against:**
- Morning model Brier (should improve as day progresses)
- Climatological baseline (season-average bracket frequencies)
- Market-price-as-probability Brier (does the market beat our model?)

### 5.2 Signal quality metrics

```sql
-- Win rate by signal type
SELECT
    signal_type,
    COUNT(*) AS n_signals,
    SUM(CASE WHEN p.pnl_net > 0 THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN p.pnl_net <= 0 THEN 1 ELSE 0 END) AS losses,
    AVG(p.pnl_net) AS mean_pnl,
    SUM(p.pnl_net) AS total_pnl
FROM intraday_signals s
JOIN paper_positions p ON p.entry_signal_id = s.signal_id
WHERE p.status IN ('exited', 'settled')
GROUP BY signal_type
```

### 5.3 Signal decomposition

Which signal contributed most to P&L?

```sql
-- Breakdown by primary reason
SELECT
    CASE
        WHEN s.reason LIKE 'dead bracket%' THEN 'truncation'
        WHEN s.hrrr_shift_f IS NOT NULL AND ABS(s.hrrr_shift_f) > 0.5 THEN 'hrrr_driven'
        WHEN s.trajectory_deviation_f IS NOT NULL AND ABS(s.trajectory_deviation_f) > 1.0 THEN 'trajectory_driven'
        WHEN s.ensemble_ratio IS NOT NULL AND ABS(s.ensemble_ratio - 1.0) > 0.15 THEN 'ensemble_driven'
        ELSE 'edge_only'
    END AS signal_source,
    COUNT(*) AS n,
    AVG(p.pnl_net) AS mean_pnl,
    SUM(p.pnl_net) AS total_pnl
FROM intraday_signals s
JOIN paper_positions p ON p.entry_signal_id = s.signal_id
WHERE p.status IN ('exited', 'settled')
GROUP BY signal_source
```

### 5.4 Paper P&L curve

```sql
SELECT
    p.event_date,
    SUM(p.pnl_net) AS daily_pnl,
    SUM(SUM(p.pnl_net)) OVER (ORDER BY p.event_date) AS cumulative_pnl,
    COUNT(*) AS n_trades
FROM paper_positions p
WHERE p.status IN ('exited', 'settled')
GROUP BY p.event_date
ORDER BY p.event_date
```

### 5.5 Comparison: morning-only vs intraday model

```sql
-- Per event_date: did the intraday model improve on the morning model?
SELECT
    m.event_date,
    AVG((m.model_prob - m.outcome)*(m.model_prob - m.outcome)) AS morning_brier,
    AVG((i.model_prob - i.outcome)*(i.model_prob - i.outcome)) AS last_intraday_brier,
    SUM(p.pnl_net) AS intraday_pnl
FROM bracket_snapshots m
LEFT JOIN (
    SELECT event_date, bracket_label, model_prob, outcome,
           ROW_NUMBER() OVER (PARTITION BY event_date, bracket_label
                              ORDER BY snapshot_ts DESC) AS rn
    FROM bracket_snapshots
    WHERE snapshot_type = 'intraday' AND outcome IS NOT NULL
) i ON m.event_date = i.event_date AND m.bracket_label = i.bracket_label AND i.rn = 1
LEFT JOIN paper_positions p ON p.event_date = m.event_date AND p.status IN ('exited','settled')
WHERE m.snapshot_type = 'morning' AND m.outcome IS NOT NULL
GROUP BY m.event_date
ORDER BY m.event_date
```

---

## 6. What the literature says about these signals

### 6.1 Observed-max truncation

This has no academic controversy — it's Bayesian updating with a degenerate likelihood. Once you observe T ≥ X, the posterior probability of T < X is zero. The only subtlety is the ASOS F→C→F rounding (±0.5°F), which is why the cutoff uses `observed_max + 0.5` rather than strict equality. Min et al. (2021, JGR) documented this rounding behavior using NY State Mesonet stations.

### 6.2 HRRR at short lead times

Benjamin et al. (2016, Monthly Weather Review) established HRRR's advantage at 0–6h lead times for surface variables, including 2m temperature. The key finding: HRRR's hourly data assimilation cycle ingests the most recent surface observations, radar reflectivity, and satellite radiances, giving it an information edge over models that cycle every 6–12 hours. For temperature at KNYC, Min et al. (2021) found HRRR carries a systematic warm and dry bias in summer (May–September) but performs well in spring/fall transitions. The lead-time-dependent blending weights proposed in §2.2 are consistent with this literature — HRRR's advantage fades beyond 6 hours.

### 6.3 Ensemble spread as uncertainty calibrator

Hamill & Colucci (1997, 1998, Monthly Weather Review) established that ensemble spread is positively correlated with forecast error — wide spread days have larger errors. However, the relationship is imperfect: ensembles are often *underdispersive* (spread too narrow relative to actual error variance), a finding confirmed for GEFS by Hamill et al. (2013). For the NYC application, the relevant finding is from Bürgi et al. (2025) on Kalshi: tail brackets are systematically overpriced. If ensemble spread correctly identifies high-uncertainty days where tails *should* be wider, and the market overprices tails regardless, the ensemble signal might actually reduce edge by making our tails wider (matching the market's already-wide tail pricing). This is an empirical question — log the ratio and check after 20 days.

### 6.4 Diurnal trajectory as forecast update

The closest academic analog is "model output statistics" (MOS) with partial-day predictors. Glahn & Lowry (1972) showed that including current observations as predictors in statistical post-processing dramatically improves same-day max temperature forecasts. The NWS's own LAMP (Local Analysis and Prediction) system uses this principle — it takes the latest observations and folds them into a regression-based forecast update. The sinusoidal trajectory model proposed here is a simplified version: instead of a full regression, it assumes the deviation from expected warming is persistent. This assumption breaks down in sea breeze events (temperature drops mid-afternoon at KNYC when onshore flow kicks in) and frontal passages.

### 6.5 Combining multiple signals

The ensemble literature (e.g., Raftery et al. 2005, Bayesian Model Averaging) suggests that combining forecasts from different sources with calibrated weights outperforms any single source. The key challenge is weight calibration — with only ~2 weeks of data, any learned weights will be noisy. The proposed approach (fixed PROVISIONAL weights, logged for later calibration) avoids overfitting and treats the first 20+ days as a learning period.

### 6.6 Favorite-longshot bias and signal timing

Bürgi et al. (2025) found that the favorite-longshot bias on Kalshi nearly disappears in the final hour before resolution. This implies that early-day signals have the most exploitable mispricing, and as settlement approaches, the market gets it right. Practically: intraday entries should happen early (morning–midday) when market prices are stale relative to our updated model; by late afternoon, the market has incorporated the same observations we're using.

---

## 7. Implementation plan

### Phase 1: Signal functions (morning_model.py additions)

1. `compute_trajectory_deviation(db_path, event_date, forecast_high, overnight_low, as_of_utc)` → `(deviation_f, confidence_factor)`
2. `apply_ensemble_width(zone_model, ens_gefs_sd, ens_ecmwf_sd, nbm_spread_raw)` → `(adjusted_zone, ratio)`
3. `combine_shifts(hrrr_shift, trajectory_shift, hrrr_weight, trajectory_confidence)` → `combined_shift_f`
4. Refactor `apply_hrrr_shift()` to accept lead-time-dependent weight
5. Update `collector.py` to call all four signals in order (§2.5)

### Phase 2: Signal generation + logging

1. Add `intraday_signals` table to schema
2. New function `generate_signals(rows, existing_positions, snapshot_ts)` → list of signal dicts
3. Log all signals to DB (including those not acted on — the signal table is exhaustive)
4. Paul alert for high-conviction signals (edge > 12%? PROVISIONAL)

### Phase 3: Paper position management

1. Add `paper_positions` table to schema
2. New function `execute_paper_trade(signal, positions, bankroll)` → updated position
3. Settlement hook in `backfill_outcome()` — after updating outcomes, also settle open positions
4. Daily paper P&L summary via Paul

### Phase 4: Evaluation queries + Datasette

1. Add all §5 queries to `metadata.yml`
2. Build a simple Python script (`scripts/evaluate_intraday.py`) that runs the full evaluation suite and prints results

### Phase 5: Calibration (after 20+ days)

1. Analyze signal decomposition — which signals actually helped?
2. Recalibrate PROVISIONAL thresholds from observed data
3. Decide whether to promote any intraday adjustments into the morning model

---

## 8. What *not* to build

- **No live order execution.** The paper engine logs signals and simulated positions. Actual Kalshi API orders are a separate, gated decision after the paper engine proves profitable.
- **No regime filter.** The morning model doesn't have one; the intraday model doesn't get one either until v0 data supports it.
- **No ML/optimization on weights.** All weights and thresholds are PROVISIONAL constants, logged for later calibration. No gradient descent, no hyperparameter search. The dataset is too small and the risk of overfitting is too high.
- **No multi-city.** The intraday engine targets KXHIGHNY only. Multi-city extension is a separate backlog item.

---

## 9. Success criteria for the intraday model

These are in addition to (not replacing) the morning v0 gate.

1. **Intraday Brier improves over the day.** By 2pm, intraday model Brier should be meaningfully better than morning model Brier for the same event_date. If it isn't, the signals aren't adding value.

2. **Paper P&L is positive after fees.** Cumulative paper P&L across 20+ days, using taker fee assumptions, should be positive. If gross P&L is positive but net is negative, the edge exists but is too small for a taker — would need to operate as a maker.

3. **Signal decomposition shows at least one profitable signal source.** If truncation signals are profitable but trajectory signals lose money, that's actionable information — keep truncation, drop trajectory.

4. **Dead-bracket selling is independently profitable.** This is the highest-conviction strategy. If even this loses money (because the market prices dead brackets at ≤2¢ and fees eat the profit), the Kalshi microstructure may not support intraday trading at all.

---

## 10. Open questions (add before building)

| Priority | Question |
|---|---|
| H | What is the actual KNYC sunrise time curve for April–May? Needed for trajectory model. Use NOAA solar calculator or hardcode monthly average. |
| H | Does the AviationWeather.gov API return temperature with enough precision for trajectory analysis? METAR reports integer °C, which rounds to ±0.5°F. |
| M | Should the paper engine use bid/ask or mid for execution price? Using mid overestimates fill quality; using bid/ask is more conservative. Recommend: ask for buys, bid for sells. |
| M | How should partial-day positions interact with the next morning's evaluation? If we're still holding a position from yesterday at 7am, does the morning model's edge assessment conflict? |
| L | Should we model slippage beyond the bid-ask spread? For thin markets with ≤$144K daily volume, large orders move the price. At paper-trade sizes (≤50 contracts), slippage is negligible. |
