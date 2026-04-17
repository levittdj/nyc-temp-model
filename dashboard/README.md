# Dashboard — v0 paper-trading review

## Run locally

```bash
streamlit run dashboard/app.py
```

Or use the wrapper script (activates venv, logs output):

```bash
./dashboard.sh
```

Default port: **8501** (`http://localhost:8501`).

## Read-only rule

The dashboard **never writes to the database**. The SQLite connection is
opened in read-only mode (`?mode=ro`). Collector and backfill are the
only writers; the dashboard must never race them.

## Scope

This is paper-trading review only — same v0 constraint as Paul alerts.
No live order execution. No model tuning. Review data, not act on it.
