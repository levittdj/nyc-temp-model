"""
Public render_* entry points for each dashboard section.

Each render_* function writes directly to `st` and returns None.
All SQL lives in dashboard/queries.py (single source of truth).
"""

from ._format import cents, to_et
from .blocked import render_blocked_signals
from .calibration import render_calibration
from .event_drilldown import render_event_drilldown
from .nbm import render_nbm_revisions
from .signals import (
    render_fee_detail,
    render_signal_decomposition,
    render_signal_type_breakdown,
)
from .trades import (
    render_equity_curve,
    render_open_positions,
    render_trade_log,
)

__all__ = [
    "cents",
    "to_et",
    "render_blocked_signals",
    "render_calibration",
    "render_equity_curve",
    "render_event_drilldown",
    "render_fee_detail",
    "render_nbm_revisions",
    "render_open_positions",
    "render_signal_decomposition",
    "render_signal_type_breakdown",
    "render_trade_log",
]
