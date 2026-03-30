# NYC Temp Model

## What this is
A bracket-probability conversion engine for Kalshi NYC daily high temperature markets.
Testing a hypothesis -- not building a known-good system.

## Core hypothesis
"Core hypothesis (updated 2026-03-30): Kalshi NYC temperature markets
systematically misallocate probability mass across brackets relative to
probabilities implied by p10/p50/p90 NBM guidance. Research suggests the
most likely direction is favorite-longshot bias -- tail brackets overpriced,
middle brackets underpriced -- consistent with the platform-wide pattern
documented in Bürgi et al. (2026) and the anecdotal participant signal.
However, weather markets may behave differently due to bot activity and
ensemble model availability. Direction remains an output of v0 data."

## Current phase
V0 -- hypothesis instrument. Three files. Paper trade. Manual review.
Goal: evidence, not profit.

## Active task
[UPDATE THIS AT THE END OF EVERY SESSION]
Phase 0 -- calibration.py + morning_model.py + logger.py done. Next: add collector.py (intraday data collection infra) + daily paper run + backfill workflow as needed.

## Build order (v0 only)
1. calibration.py  -- 3yr KNYC ASOS + ERA5, OLS regression, output nbm_bias to config.json
2. morning_model.py -- NBM p10/p50/p90 + Kalshi prices, zone interpolation, log edge per bracket
3. logger.py       -- SQLite, 8 fields per bracket-day, manual terminal output
4. collector.py    -- intraday snapshots (prices + model_prob); additive analysis infra only

## Key constraints -- do not violate
- NBM is the full forecast. No separate sky cover / wind / RH adjustments on top.
- v0 logs everything, filters nothing, applies no thresholds.
- Log both positive and negative edge. No directional bias in the code.
- Kalshi NHIGH brackets use strict inequality on tails and inclusive between brackets (confirmed from CFTC filing). The continuous model applies a half-degree continuity correction to make boundaries contiguous. model_prob must sum to 1.0 (enforced by assertion).
- event_date is the calendar date whose high temperature the market settles on. snapshot_ts is when data was captured. These are different and must never be conflated. A 7am run on April 2nd logging April 2nd's market has event_date=2026-04-02 and forecast_lead_hours~7. The same run logging April 3rd's market has event_date=2026-04-03 and forecast_lead_hours~31.
- v0 evaluation uses snapshot_type='morning' rows ONLY. Intraday snapshots are analysis context, not evaluation data.
- All numeric thresholds (3F record proximity, 20-day window, 60% persistence) are
  PROVISIONAL placeholders. Label them as such. Do not treat them as validated.
- Do not build regime filter, intraday trading engine, or Telegram alerts in v0. (collector.py data collection is allowed.)

## v0 success gate (must pre-register before evaluating)
Layer 1 -- Brier score better than naive normal distribution baseline
Layer 2 -- Mean edge shows repeatable directional pattern (permutation test p < 0.10)
Layer 3 -- After-cost paper P&L positive vs zero and vs NBM p50 bracket baseline
Failure at each layer has a different diagnosis. Do not collapse to "no edge."

## Files
config.json          -- nbm_bias written by calibration.py (single float)
calibration.py       -- one-time pre-launch script
morning_model.py     -- core v0 model
logger.py            -- SQLite log + terminal output
records.json         -- all-time KNYC daily records (one-time build)

## Source of truth
project_docs.html    -- strategy, hypothesis, decisions log, build tracker, risk
Do not rely on lean_v1_spec.html -- it is stale and contradicts current decisions.

## Weak prior (informational only)
A participant has reportedly made money buying high-probability middle brackets and
fading tails. One anecdotal data point. Justifies removing directional assumptions
from v0. Does not set the new direction. Let the data speak.
