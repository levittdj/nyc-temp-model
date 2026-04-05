#!/usr/bin/env python3
"""
Validation harness for DSM/CLI monitoring and backfill preference.

What this validates:
1) DSM parser handles coded DSM text.
2) CLI parser handles preliminary and final CLI text.
3) Logger table writers/readers work in a temp SQLite DB.
4) logger.py backfill with --actual-max updates bracket_snapshots (no network).

Run:
  python3 scripts/validate_dsm_cli.py
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import collector
import logger


@dataclass
class _Row:
    ticker: str
    title: str
    bracket_label: str
    lower_f: float
    upper_f: float
    market_price: float
    model_prob: float
    edge: float
    market_bid: float
    market_ask: float


DSM_SAMPLE = """925
CXUS41 KOKX 310515
DSMNYC
KNYC DS 30/03 731351/ 470340// 73/ 47//0041828/T/00/00/00/00/00/00/
00/00/00/00/00/00/00/00/00/00/00/T/00/00/00/00/00/00/M/24152036/
24301455/N/NN/N/N/NN/EW
"""

CLI_FINAL_SAMPLE = """CDUS41 KOKX 310622
CLINYC

CLIMATE REPORT
NATIONAL WEATHER SERVICE NEW YORK NY
222 AM EDT TUE MAR 31 2026

...THE NEW YORK CITY NY CLIMATE SUMMARY FOR MARCH 30 2026...

DAILY CLIMATE DATA
MAXIMUM TEMPERATURE (F)   74        86      1998
"""

CLI_PRELIM_SAMPLE = """CDUS41 KOKX 302130
CLINYC

PRELIMINARY CLIMATE DATA
NATIONAL WEATHER SERVICE NEW YORK NY
530 PM EDT MON MAR 30 2026

...THE NEW YORK CITY NY CLIMATE SUMMARY FOR MARCH 30 2026...

DAILY CLIMATE DATA
MAXIMUM TEMPERATURE (F)   73
"""


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def _build_rows() -> list[_Row]:
    return [
        _Row("T62", "<62", "<62", float("-inf"), 61.5, 0.01, 0.00, -0.01, 0.0, 0.02),
        _Row("B62.5", "62-63", "62-63", 61.5, 63.5, 0.03, 0.03, 0.00, 0.02, 0.04),
        _Row("B64.5", "64-65", "64-65", 63.5, 65.5, 0.05, 0.06, 0.01, 0.04, 0.06),
        _Row("B66.5", "66-67", "66-67", 65.5, 67.5, 0.15, 0.14, -0.01, 0.14, 0.16),
        _Row("B68.5", "68-69", "68-69", 67.5, 69.5, 0.20, 0.18, -0.02, 0.19, 0.21),
        _Row("T69", ">69", ">69", 69.5, float("inf"), 0.56, 0.59, 0.03, 0.55, 0.57),
    ]


def main() -> int:
    fetch_ts = datetime(2026, 3, 31, 6, 30, tzinfo=timezone.utc)

    # 1) Parser fixture tests
    dsm = collector._parse_dsm_observation(DSM_SAMPLE, fetch_ts)
    _assert(dsm is not None, "DSM parser failed on coded sample")
    dsm_issuance, dsm_event, dsm_high = dsm
    _assert(dsm_event == date(2026, 3, 30), f"Unexpected DSM event_date: {dsm_event}")
    _assert(dsm_high == 73.0, f"Unexpected DSM running_high_f: {dsm_high}")
    _assert(dsm_issuance is not None, "DSM issuance timestamp missing")

    cli_final = collector._parse_cli_observation(CLI_FINAL_SAMPLE, fetch_ts)
    _assert(cli_final is not None, "CLI final parser failed")
    c_iss, c_event, c_high, c_pre = cli_final
    _assert(c_event == date(2026, 3, 30), f"Unexpected final CLI event_date: {c_event}")
    _assert(c_high == 74.0, f"Unexpected final CLI high: {c_high}")
    _assert(c_pre is False, "Final CLI incorrectly marked preliminary")
    _assert(c_iss is not None, "Final CLI issuance timestamp missing")

    cli_pre = collector._parse_cli_observation(CLI_PRELIM_SAMPLE, fetch_ts)
    _assert(cli_pre is not None, "CLI preliminary parser failed")
    _, p_event, p_high, p_pre = cli_pre
    _assert(p_event == date(2026, 3, 30), f"Unexpected preliminary CLI event_date: {p_event}")
    _assert(p_high == 73.0, f"Unexpected preliminary CLI high: {p_high}")
    _assert(p_pre is True, "Preliminary CLI not marked preliminary")

    # 2-4) Integration against temp DB
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "temp.sqlite"
        target = date(2026, 3, 30)
        rows = _build_rows()
        logger.log_morning_run(
            db_path=db,
            event_date=target,
            rows=rows,
            pct_f_raw=(66.0, 70.0, 74.0),
            nbm_bias=0.2,
            record_prox_flag=False,
            nws_log_context={},
            snapshot_ts_utc=fetch_ts,
            snapshot_type="morning",
            nbp_meta={},
            records_path=None,
        )

        changed0 = logger.log_dsm_observation(
            db_path=db,
            fetch_ts_utc=fetch_ts,
            issuance_ts_utc=dsm_issuance,
            event_date=dsm_event,
            running_high_f=73.0,
            raw_text=DSM_SAMPLE,
        )
        _assert(changed0 is False, "First DSM observation should not be flagged as changed")

        changed1 = logger.log_dsm_observation(
            db_path=db,
            fetch_ts_utc=fetch_ts.replace(minute=45),
            issuance_ts_utc=dsm_issuance,
            event_date=dsm_event,
            running_high_f=74.0,
            raw_text=DSM_SAMPLE,
        )
        _assert(changed1 is True, "DSM high change should be flagged")

        first_cli = logger.log_cli_observation(
            db_path=db,
            fetch_ts_utc=fetch_ts,
            issuance_ts_utc=c_iss,
            event_date=target,
            cli_high_f=74.0,
            is_preliminary=False,
            raw_text=CLI_FINAL_SAMPLE,
        )
        _assert(first_cli is True, "First CLI observation should be flagged as first")

        best_cli = logger.latest_cli_observed_high(db, target, final_only=True)
        _assert(best_cli == 74.0, f"Expected final CLI high 74.0, got {best_cli}")

        # End-to-end backfill via logger.py CLI (IEM fetch not exercised here).
        proc = subprocess.run(
            [
                "python3",
                str(Path(__file__).resolve().parents[1] / "logger.py"),
                "--db",
                str(db),
                "--actual-max",
                "74",
                target.isoformat(),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        _assert(proc.returncode == 0, f"logger.py CLI failed: {proc.stderr}\n{proc.stdout}")
        _assert(
            "Using manually specified actual_max_f: 74" in proc.stdout,
            "Backfill did not use --actual-max as expected",
        )

    print("PASS: DSM/CLI parser + DB + backfill validation")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
