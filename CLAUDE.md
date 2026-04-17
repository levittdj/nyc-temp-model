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
V0 -- **paper + review**. calibration + morning_model + logger + collector all live on VM cron. The intraday paper-trading engine (`intraday_engine.py`) is shipped in v0 — it writes `intraday_signals` + `paper_positions` with a Kalshi taker fee model and never hits the Kalshi trade endpoint. `snapshot_type='morning'` rows remain the pre-registered v0 evaluation baseline regardless of paper P&L.
Datasette (port 8001) + Streamlit paper-trade dashboard (port 8501) are the two review surfaces. Telegram alert fleet (10 Paul scripts) is live.
Goal: evidence, not profit.

## Active task
[UPDATE THIS AT THE END OF EVERY SESSION]
**Shipped in repo (as of 2026-04-16):**
- **Quint (5-knot) zone CDF is now the primary `model_prob` path**; legacy 3-knot is a fallback and emits a stderr `WARNING` when it fires. `model_prob_triplet_cdf` is still logged for A/B in Datasette.
- Open-Meteo GEFS/ECMWF ensemble spread columns (logging-only); intraday observed-max truncation + HRRR shift (collector only); NBP p25/p75/SD; Datasette canned queries for triplet-vs-quint `model_prob`, edge by `forecast_lead_hours`, ensemble spread vs |error|, intraday Brier/truncation impact, `quint_cdf_availability`, `brier_by_bracket_position`, and paper-trading queries (`paper_pnl_daily`, `paper_signal_quality`, `paper_position_history`, `paper_signal_decomposition`).
- **Paper-trading intraday engine** — `intraday_engine.py` generates BUY_YES / SELL_YES / EXIT signals per intraday tick, respects an entry-time window + cooldown, applies Kalshi taker fees, and settles on CLI backfill.
- **Four PROVISIONAL guardrails** in `intraday_engine.py` from the 6-day / 71-position paper review that landed net **−$45.70** (see `paper_trading_fixes.md`): `BUY_YES_MIN_MARKET_PRICE = 0.40`, `DEAD_BRACKET_FLOOR = 0.04` (was 0.02), `SELL_YES_MAX_MARKET_PRICE = 0.55`, `MAX_DAILY_ENTRIES_PER_BRACKET = 1`. Blocked candidates still log to `intraday_signals` with `executed=0` so counterfactual P&L is measurable. Do not remove until the CDF rebuild ships and is evaluated on fresh settled rows.
- **Streamlit dashboard** (`dashboard/`, launched by `dashboard.sh` on VM :8501, read-only via `file:...?mode=ro` URI). Live sections: timeframe filter, 5 KPIs, equity curve, trade log, open positions (live unrealized P&L), signal decomposition, signal-type breakdown, fee detail, per-event drill-down page. **Temporarily disabled on main page** (commented out in `dashboard/app.py` pending re-enable, render functions still present in `dashboard/sections/`): calibration scatter, NBM revisions, blocked signals. All timestamps rendered in ET; monetary columns as `$X.XX` via `st.column_config.NumberColumn`. DB still stores UTC ISO-8601.
- **Paul Telegram alert fleet (10 scripts)** on VM cron: `paul_morning_edge`, `paul_settlement`, `paul_collector_health`, `paul_backfill_check`, `paul_price_move` (incl. sweep), `paul_conviction` (70% first cross), `paul_intraday_edge`, `paul_paper_signals`, `paul_730pm_lotto`, `paul_metar_check` / `paul_metar_react`. All thresholds PROVISIONAL. No order execution from core repo.

**Next queued work:**
1. **CDF tail-shape investigation** — mirror-reflected tails (`L = 2·p10 − p25`, `U = 2·p90 − p75`) appear to over-assign mass to far-from-center brackets. Plan: (a) run `market_as_probability_brier` canned query — a Layer-1 failure would mean calibration must be fixed before any mispricing claim; (b) A/B `model_prob` vs `model_prob_triplet_cdf` on settled rows; (c) prototype a parametric tail (skew-normal or `TXNSD`-driven).
2. **Run 20–30 settled days** with the current guardrails + review daily in the dashboard; re-evaluate the v0 gate.

**V0 backlog:** extend the pipeline to additional Kalshi daily-high-temperature cities (collect + settlement backfill per locale). v0 evaluation should stratify by city/series — the model may work in some locales and not others; measure it rather than assuming NYC generalizes. See build tracker in project_docs.html (COLLECT phase).

## Build order (v0)
1. calibration.py     -- 3yr KNYC ASOS + ERA5, OLS regression, output nbm_bias to config.json
2. morning_model.py   -- NBM p10/p50/p90 (+ p25/p75/SD) + Kalshi prices, 5-knot CDF, log edge per bracket
3. logger.py          -- SQLite bracket_snapshots + backfill (`intraday_signals`, `paper_positions` also live here via intraday_engine)
4. collector.py       -- intraday snapshots (prices + model_prob + HRRR/observed-max truncation) + DSM/CLI/METAR monitoring
5. intraday_engine.py -- paper-trading engine (v0, no live execution); writes intraday_signals + paper_positions
6. dashboard/         -- Streamlit read-only review UI (:8501); single source of SQL in dashboard/queries.py

## Key constraints -- do not violate
- `model_prob` may combine NBM with other forecast or observational inputs when implemented; log enough columns to trace what drove each snapshot, and avoid silent double-counting when a signal is already embedded in NBM.
- v0 logs everything, filters nothing, applies no thresholds.
- Log both positive and negative edge. No directional bias in the code.
- Outcome backfill (actual_max_f + outcome) is as critical as data collection. A day without a backfilled outcome is a wasted day. The backfill cron runs at 8am ET daily. If it fails, manually run `python logger.py YYYY-MM-DD` before the next morning model run.
- DSM, CLI, and METAR in collector.py are always logged for timing / backfill / analysis; they may also feed probability computation when explicitly wired in code.
- Kalshi NHIGH brackets use strict inequality on tails and inclusive between brackets (confirmed from CFTC filing). The continuous model applies a half-degree continuity correction to make boundaries contiguous. model_prob must sum to 1.0 (enforced by assertion).
- event_date is the calendar date whose high temperature the market settles on. snapshot_ts is when data was captured. These are different and must never be conflated. A 7am run on April 2nd logging April 2nd's market has event_date=2026-04-02 and forecast_lead_hours~7. The same run logging April 3rd's market has event_date=2026-04-03 and forecast_lead_hours~31.
- v0 evaluation uses snapshot_type='morning' rows ONLY. Intraday snapshots are analysis context, not evaluation data.
- All numeric thresholds (3F record proximity, 20-day window, 60% persistence) are
  PROVISIONAL placeholders. Label them as such. Do not treat them as validated.
- Do not build regime filter in v0. Intraday trading engine is in scope — implement against collector/DB; start paper/simulation and keep **live order execution** explicit, gated, and separate from evaluation rows (`snapshot_type='morning'` remains the v0 metric baseline).
- The dashboard is strictly read-only. Never add write paths, never add background refresh that mutates the DB, never add counterfactual P&L for blocked signals (out of scope). All dashboard SQL must live in `dashboard/queries.py`; if a query is duplicated in `metadata.yml`, copy it as a comment so the two stay in sync.
- PROVISIONAL thresholds referenced in code (e.g. 2°F NBM-revision, 40¢ BUY_YES floor, 55¢ SELL_YES ceiling, 4¢ dead-bracket floor, 1-entry-per-bracket cap) must stay tagged `# PROVISIONAL` with a comment pointing back to the originating file so future removals are traceable.

## v0 success gate (must pre-register before evaluating)
Layer 1 -- Brier score better than naive normal distribution baseline
Layer 2 -- Mean edge shows repeatable directional pattern (permutation test p < 0.10)
Layer 3 -- After-cost paper P&L positive vs zero and vs NBM p50 bracket baseline
Failure at each layer has a different diagnosis. Do not collapse to "no edge."

## Files
config.json          -- nbm_bias written by calibration.py (single float)
calibration.py       -- one-time pre-launch script
morning_model.py     -- core v0 model (5-knot CDF primary)
logger.py            -- SQLite log + terminal output + outcome backfill
collector.py         -- intraday snapshots + DSM/CLI/METAR monitoring tables (+ HRRR shift, observed-max truncation)
intraday_engine.py   -- v0 paper-trading engine; writes intraday_signals + paper_positions (NO live execution)
records.json         -- all-time KNYC daily records (one-time build)
dashboard/           -- Streamlit read-only review UI; SQL lives in dashboard/queries.py only
metadata.yml         -- Datasette canned queries (sync with dashboard/queries.py on semantic changes)
paper_trading_fixes.md -- rationale + code for the four 2026-04-15 PROVISIONAL guardrails

## VM deployment (v0 ops)
- VM runs collector.py via crontab (every 30 min, 6am-midnight ET)
- morning_model.py runs on **cron** at ~7am ET (wrapper `morning_model.sh`: venv + log append)
- intraday_engine.py runs on the same intraday cadence as collector; same wrapper pattern
- Full Paul fleet runs on cron (see Active task section for the 10 scripts)
- Database: data/tempmodel.db on the VM (also referenced as nyc_temp_log.sqlite at repo root for local dev)
- Logs: logs/collector.log, logs/morning_model.log, logs/dashboard.log on the VM
- Health check: logs/last_success heartbeat, checked every 2 hours; paul_collector_health also alerts
- **Datasette** on port 8001 — general browser + canned queries; http://<vm-ip>:8001
- **Streamlit dashboard** on port 8501 — paper-trade review (read-only); launched by `dashboard.sh`
- Read-only access to the live SQLite database (dashboard enforces via `file:...?mode=ro`)
- Code updates: manual git pull on VM after testing locally
- Monitor morning_model + intraday_engine daily for data quality (fresh morning snapshot, new paper_positions/intraday_signals rows, no API-stale output)

## Source of truth
project_docs.html    -- strategy, hypothesis, decisions log, build tracker, risk
Do not rely on lean_v1_spec.html -- it is stale and contradicts current decisions.

## Weak prior (informational only)
A participant has reportedly made money buying high-probability middle brackets and
fading tails. One anecdotal data point. Justifies removing directional assumptions
from v0. Does not set the new direction. Let the data speak.
