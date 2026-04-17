"""
Plotly styling shared across the dashboard.

Every Plotly figure in the dashboard should be passed through
`apply_dark_template(fig)` before `st.plotly_chart(fig, ...)` so
charts share the project_docs.html palette / type.
"""

from __future__ import annotations

from typing import Any, Dict


PLOTLY_TEMPLATE_DARK: Dict[str, Any] = {
    "paper_bgcolor": "#0a0e14",
    "plot_bgcolor": "#0f1520",
    "font": {
        "family": "'IBM Plex Mono', monospace",
        "size": 11,
        "color": "#c8d8f0",
    },
    "colorway": [
        "#f0a500",
        "#3b82f6",
        "#22c55e",
        "#a855f7",
        "#14b8a6",
        "#ef4444",
    ],
    "xaxis": {
        "gridcolor": "#1e2d45",
        "zerolinecolor": "#243450",
        "tickfont": {"size": 10, "color": "#7a90b0"},
    },
    "yaxis": {
        "gridcolor": "#1e2d45",
        "zerolinecolor": "#243450",
        "tickfont": {"size": 10, "color": "#7a90b0"},
    },
    "legend": {
        "font": {"color": "#c8d8f0"},
        "bgcolor": "#0f1520",
        "bordercolor": "#1e2d45",
        "borderwidth": 1,
    },
}


_SECONDARY_AXIS_STYLE: Dict[str, Any] = {
    "gridcolor": "#1e2d45",
    "zerolinecolor": "#243450",
    "tickfont": {"size": 10, "color": "#7a90b0"},
}


def apply_dark_template(fig):
    """Apply the project-wide dark template to a Plotly figure in-place.

    Returns the figure so the call can be chained (`st.plotly_chart(apply_dark_template(fig))`).

    Also mirrors the primary axis styling onto any secondary axes
    (yaxis2, xaxis2, ...) that the figure has explicitly configured.
    """
    fig.update_layout(**PLOTLY_TEMPLATE_DARK)

    # Mirror styling onto secondary axes that traces actually use.
    for attr in ("yaxis2", "yaxis3", "xaxis2", "xaxis3"):
        axis = getattr(fig.layout, attr, None)
        if axis is None:
            continue
        # Only touch axes the figure has configured (overlaying or titled).
        has_overlay = getattr(axis, "overlaying", None)
        has_title = bool(getattr(getattr(axis, "title", None), "text", None))
        if has_overlay or has_title:
            fig.update_layout({attr: _SECONDARY_AXIS_STYLE})

    return fig
