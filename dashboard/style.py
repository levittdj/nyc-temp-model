"""
Global visual treatment for the Streamlit dashboard.

Call `inject_global_css()` at the top of each Streamlit entry point
(app.py and every file in dashboard/pages/) so the same palette / type /
density shows up everywhere.

Use `section_header(mono_label, title)` in place of `st.subheader` to
mirror the eyebrow + title pattern from project_docs.html.

Palette lifted verbatim from the CSS variables in project_docs.html
(--bg, --bg2, --bg3, --border, --amber, --text, --text2, --text3).
"""

from __future__ import annotations

import streamlit as st


_GLOBAL_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

/* Body type: IBM Plex Sans, matching project_docs.html */
html, body, .stApp, [data-testid="stAppViewContainer"] {
    font-family: 'IBM Plex Sans', sans-serif;
}

/* Subtle scanline overlay (project_docs body::before) */
.stApp::before {
    content: '';
    position: fixed;
    inset: 0;
    pointer-events: none;
    z-index: 999;
    background: repeating-linear-gradient(
        0deg,
        transparent,
        transparent 2px,
        rgba(0, 0, 0, 0.025) 2px,
        rgba(0, 0, 0, 0.025) 4px
    );
}

/* Tighten top padding so pages feel dense like project_docs */
[data-testid="stAppViewContainer"] .main .block-container {
    padding-top: 1.25rem;
    padding-bottom: 3rem;
}

/* Eyebrow utility (above section_header titles) */
.mono-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 9px;
    font-weight: 600;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: #3a5070;
    margin: 18px 0 4px;
}
.mono-label:first-child { margin-top: 0; }

/* section_header h2 — matches project_docs .panel-title */
.section-title {
    margin: 0 0 12px 0;
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 20px;
    font-weight: 600;
    color: #c8d8f0;
    line-height: 1.2;
}

/* Metric tiles: .card treatment from project_docs */
div[data-testid="stMetric"] {
    background: #0f1520;
    border: 1px solid #1e2d45;
    border-radius: 6px;
    padding: 12px;
}

/* Dataframe headers: matches project_docs .t th */
[data-testid="stDataFrame"] thead tr th,
[data-testid="stTable"] thead tr th {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 9px;
    font-weight: 600;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: #3a5070;
}

/* Expander header: same eyebrow typography */
[data-testid="stExpander"] summary p,
[data-testid="stExpander"] summary span {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    letter-spacing: 0.08em;
    color: #f0a500;
}
</style>
"""


def inject_global_css() -> None:
    """Emit the project-wide CSS block once per page load."""
    st.markdown(_GLOBAL_CSS, unsafe_allow_html=True)


def section_header(mono_label: str, title: str) -> None:
    """Render the project_docs eyebrow + title pattern above a section.

    Example: section_header("TRADES", "Trade log")
    """
    safe_label = str(mono_label).upper()
    st.markdown(
        f'<div class="mono-label">{safe_label}</div>'
        f'<h2 class="section-title">{title}</h2>',
        unsafe_allow_html=True,
    )
