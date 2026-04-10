import sqlite3, requests, sys
from datetime import date, datetime, timezone

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

_NY = ZoneInfo('America/New_York')

def _et(ts) -> str:
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(_NY).strftime('%-I:%M%p ET')

DB = '/home/ubuntu/nyc-temp-model/nyc_temp_log.sqlite'
BOT_TOKEN = '8652874695:AAFie5ef1mj7YXFeCs1yFiDqOEO4A76Ekg4'
CHAT_ID = -5229782521

conn = sqlite3.connect(DB)
today = date.today().isoformat()

# Find the most recent intraday snapshot for today
latest_ts = conn.execute('''
    SELECT MAX(snapshot_ts) FROM bracket_snapshots
    WHERE event_date=? AND snapshot_type='intraday'
''', (today,)).fetchone()[0]

if not latest_ts:
    sys.exit(0)

# Check if this snapshot captured a new METAR
new_obs = conn.execute('''
    SELECT metar_new_obs FROM bracket_snapshots
    WHERE event_date=? AND snapshot_ts=? LIMIT 1
''', (today, latest_ts)).fetchone()

if not new_obs or not new_obs[0]:
    sys.exit(0)

# Fetch running high and bracket data from this snapshot
rows = conn.execute('''
    SELECT bracket_label, bracket_lower_f, bracket_upper_f,
           market_price, model_prob, edge, observed_max_f_at_snapshot
    FROM bracket_snapshots
    WHERE event_date=? AND snapshot_ts=? AND snapshot_type='intraday'
    ORDER BY bracket_lower_f
''', (today, latest_ts)).fetchall()

if not rows:
    sys.exit(0)

running_high = rows[0][6]

# Latest METAR reading
metar_row = conn.execute('''
    SELECT observation_ts, tmpf FROM metar_observations
    WHERE station='KNYC'
    ORDER BY observation_ts DESC LIMIT 1
''').fetchone()

metar_ts = metar_row[0] if metar_row else '?'
metar_tmpf = metar_row[1] if metar_row else None

lines = ['METAR ' + today + '  ' + _et(metar_ts)]
if metar_tmpf is not None:
    lines.append('Current: ' + str(int(metar_tmpf)) + 'F')
if running_high is not None:
    lines.append('Running high: ' + str(int(running_high)) + 'F')
lines.append('')

# Show brackets — highlight any zeroed by truncation
for bracket_label, lower_f, upper_f, mkt, model, edge, _ in rows:
    if model == 0.0 and lower_f is not None and upper_f is not None and running_high is not None:
        # Truncated bracket
        if upper_f <= running_high:
            lines.append('[x] ' + bracket_label + '  killed by running high')
            continue
    bar = '[+]' if edge is not None and edge > 0 else '[-]'
    edge_s = ('+' if edge > 0 else '') + str(round(edge * 100, 1)) + '%' if edge is not None else '?'
    lines.append(bar + ' ' + bracket_label.ljust(8) + '  mkt ' + str(int(mkt * 100)) + 'c  model ' + str(int(model * 100)) + '%  edge ' + edge_s)

requests.post('https://api.telegram.org/bot' + BOT_TOKEN + '/sendMessage',
    json={'chat_id': CHAT_ID, 'text': '\n'.join(lines)})
