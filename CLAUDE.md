# NYC Temp Model

## What this is
A bracket-probability conversion engine for Kalshi NYC daily high temperature markets.
Testing a hypothesis -- not building a known-good system.

## Core hypothesis
"Kalshi NYC temperature markets may systematically misallocate probability mass across
brackets relative to probabilities implied by p10/p50/p90 NBM guidance."
Direction unknown. Magnitude unknown. Whether it exists at all: unknown.
Do not assume tails are underpriced. Do not assume middles are underpriced.
The data answers the direction question.

## Current phase
V0 -- hypothesis instrument. Three files. Paper trade. Manual review.
Goal: evidence, not profit.

## Active task
[UPDATE THIS AT THE END OF EVERY SESSION]
Phase 0 -- calibration.py + morning_model.py done. Next: logger.py.

## Build order (v0 only)
1. calibration.py  -- 3yr KNYC ASOS + ERA5, OLS regression, output nbm_bias to config.json
2. morning_model.py -- NBM p10/p50/p90 + Kalshi prices, zone interpolation, log edge per bracket
3. logger.py       -- SQLite, 8 fields per bracket-day, manual terminal output

## Key constraints -- do not violate
- NBM is the full forecast. No separate sky cover / wind / RH adjustments on top.
- v0 logs everything, filters nothing, applies no thresholds.
- Log both positive and negative edge. No directional bias in the code.
- All numeric thresholds (3F record proximity, 20-day window, 60% persistence) are
  PROVISIONAL placeholders. Label them as such. Do not treat them as validated.
- Do not build regime filter, intraday engine, or Telegram alerts in v0.

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
