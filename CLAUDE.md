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

**Secondary v0 hypothesis (H3):** Edge and economic attractiveness are not assumed uniform across daily-high temperature markets (cities / Kalshi series). Some locales may be far more mispriced or tradeable than others; v0 evaluation should stratify by market once multi-city data exists. See Hypothesis tab in project_docs.html.

## Current phase
V0 -- hypothesis instrument. calibration + morning_model + logger + collector. Paper trade.
Morning model: **automated** on VM cron (~7am ET, e.g. `morning_model.sh`) — evaluation rows are still `snapshot_type='morning'` only. Collector: VM cron for intraday data.
Datasette is up for data review. Telegram is live.
Goal: evidence, not profit.

## Active task
[UPDATE THIS AT THE END OF EVERY SESSION]
Core pipeline is running on the VM. **Next ops:** add **cron-scheduled Paul (agent) runs** that proactively push Telegram when something worth attention happens — e.g. large intraday edge vs last snapshot, sharp Kalshi price move between collector ticks, DSM/CLI issuance events (from DB tables), or a digest after morning_model. Keep all alert thresholds **PROVISIONAL**; Paul nudges paper decisions only — no order execution from this repo in v0. See project_docs RUN phase (Paul task).
**V0 backlog:** extend the pipeline to additional Kalshi daily-high-temperature cities (collect + settlement backfill per locale). v0 evaluation should stratify by city/series — the model may work in some locales and not others; measure it rather than assuming NYC generalizes. See build tracker in project_docs.html (COLLECT phase).

## Build order (v0 only)
1. calibration.py  -- 3yr KNYC ASOS + ERA5, OLS regression, output nbm_bias to config.json
2. morning_model.py -- NBM p10/p50/p90 + Kalshi prices, zone interpolation, log edge per bracket
3. logger.py       -- SQLite bracket_snapshots (event_date + snapshot_ts + snapshot_type + fields); stderr table; invoked from morning_model + collector
4. collector.py    -- intraday snapshots (prices + model_prob); additive analysis infra only

## Key constraints -- do not violate
- NBM is the full forecast. separate sky cover / wind / RH adjustments as a P1 on top
- v0 logs everything, filters nothing, applies no thresholds.
- Log both positive and negative edge. No directional bias in the code.
- Outcome backfill (actual_max_f + outcome) is as critical as data collection. A day without a backfilled outcome is a wasted day. The backfill cron runs at 8am ET daily. If it fails, manually run `python logger.py YYYY-MM-DD` before the next morning model run.
- DSM and CLI monitoring in collector.py is for information timing analysis only. These are NOT model inputs. The morning model uses NBM percentiles only. Do not add DSM/CLI data to the probability computation in v0.
- Kalshi NHIGH brackets use strict inequality on tails and inclusive between brackets (confirmed from CFTC filing). The continuous model applies a half-degree continuity correction to make boundaries contiguous. model_prob must sum to 1.0 (enforced by assertion).
- event_date is the calendar date whose high temperature the market settles on. snapshot_ts is when data was captured. These are different and must never be conflated. A 7am run on April 2nd logging April 2nd's market has event_date=2026-04-02 and forecast_lead_hours~7. The same run logging April 3rd's market has event_date=2026-04-03 and forecast_lead_hours~31.
- v0 evaluation uses snapshot_type='morning' rows ONLY. Intraday snapshots are analysis context, not evaluation data.
- All numeric thresholds (3F record proximity, 20-day window, 60% persistence) are
  PROVISIONAL placeholders. Label them as such. Do not treat them as validated.
- Do not build regime filter, intraday trading engine in v0. (collector.py data collection is allowed.)

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
collector.py         -- intraday snapshots + DSM/CLI monitoring tables
(collector.py also writes to dsm_observations and cli_observations tables for NWS product monitoring)
records.json         -- all-time KNYC daily records (one-time build)

## VM deployment (v0 ops)
- VM runs collector.py via crontab (every 30 min, 6am-midnight ET)
- morning_model.py runs on **cron** at ~7am ET (wrapper e.g. `morning_model.sh`: venv + log append)
- Optional: separate crontab lines for **Paul** (agent) to poll SQLite / logs and send Telegram alerts (see project_docs)
- Database: data/tempmodel.db on the VM
- Logs: logs/collector.log on the VM
- Health check: logs/last_success heartbeat, checked every 2 hours
- Datasette runs on port 8001 for team data review
  http://<vm-ip>:8001
- Canned queries in metadata.yml -- no SQL needed to review daily output
- Read-only access to the live SQLite database
- Code updates: manual git pull on VM after testing locally
- Monitor morning_model runs daily for data quality (fresh snapshot, snapshot_type=morning, no API-stale output)

## Source of truth
project_docs.html    -- strategy, hypothesis, decisions log, build tracker, risk
Do not rely on lean_v1_spec.html -- it is stale and contradicts current decisions.

## Weak prior (informational only)
A participant has reportedly made money buying high-probability middle brackets and
fading tails. One anecdotal data point. Justifies removing directional assumptions
from v0. Does not set the new direction. Let the data speak.
