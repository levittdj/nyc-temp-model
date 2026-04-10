import sqlite3, requests, sys
from datetime import datetime, timezone
from collections import defaultdict

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
    return ts.astimezone(_NY).strftime('%-I:%M%p ET').lower().replace('am', 'am').replace('pm', 'pm')

DB = '/home/ubuntu/nyc-temp-model/nyc_temp_log.sqlite'
BOT_TOKEN = '8652874695:AAFie5ef1mj7YXFeCs1yFiDqOEO4A76Ekg4'
CHAT_ID = -5229782521
THRESHOLD = 0.15       # PROVISIONAL: 15 cents, ~p97 of observed tick moves (5-day sample)
SWEEP_THRESHOLD = 0.10 # PROVISIONAL: 10 cents per bracket for multi-bracket sweep
SWEEP_MIN_BRACKETS = 3 # PROVISIONAL: minimum brackets moving together

conn = sqlite3.connect(DB)

recent_ts = conn.execute('''
    SELECT DISTINCT snapshot_ts FROM bracket_snapshots
    WHERE snapshot_type='intraday'
    ORDER BY snapshot_ts DESC LIMIT 2
''').fetchall()

if len(recent_ts) < 2:
    sys.exit(0)

ts_new, ts_old = recent_ts[0][0], recent_ts[1][0]

new_rows = {(r[0], r[1]): r for r in conn.execute('''
    SELECT event_date, bracket_label, market_price, model_prob, edge
    FROM bracket_snapshots
    WHERE snapshot_type='intraday' AND snapshot_ts=?
''', (ts_new,)).fetchall()}

old_rows = {(r[0], r[1]): r for r in conn.execute('''
    SELECT event_date, bracket_label, market_price, model_prob, edge
    FROM bracket_snapshots
    WHERE snapshot_type='intraday' AND snapshot_ts=?
''', (ts_old,)).fetchall()}

# Compute all moves
all_moves = []  # (event_date, bracket_label, old_p, new_p, move, model_prob, edge)
for key, new in new_rows.items():
    if key not in old_rows:
        continue
    old = old_rows[key]
    if new[2] is None or old[2] is None:
        continue
    move = new[2] - old[2]
    all_moves.append((key[0], key[1], old[2], new[2], move, new[3], new[4]))

# Detect sweeps: 3+ brackets same event_date each moving >=SWEEP_THRESHOLD (any direction)
# Prices sum to ~1, so they can't all move the same way — a redistribution across
# multiple brackets is what indicates new information repricing the whole distribution.
moves_by_date = defaultdict(list)
for row in all_moves:
    moves_by_date[row[0]].append(row)

sweep_alerts = []
for event_date, rows in moves_by_date.items():
    large_movers = [r for r in rows if abs(r[4]) >= SWEEP_THRESHOLD]
    if len(large_movers) >= SWEEP_MIN_BRACKETS:
        sweep_alerts.append((event_date, large_movers, rows))

# Detect individual large moves
alerts = []
for row in all_moves:
    event_date, label, old_p, new_p, move, model_prob, edge = row
    if abs(move) >= THRESHOLD:
        alerts.append((abs(move), event_date, label, old_p, new_p, move, model_prob, edge))
alerts.sort(reverse=True)

if not alerts and not sweep_alerts:
    sys.exit(0)

lines = ['Sharp price move — ' + _et(ts_new)]

# Sweep alerts first — higher signal
for event_date, large_movers, all_date_rows in sweep_alerts:
    total_brackets = len(all_date_rows)
    lines.append('')
    lines.append('SWEEP: ' + str(len(large_movers)) + '/' + str(total_brackets) + ' brackets moved \u226510c  (' + event_date + ')')
    lines.append('  Distribution repriced across multiple brackets — indicates new information, not noise.')
    for r in sorted(large_movers, key=lambda x: x[1]):
        move = r[4]
        lines.append('  ' + r[1] + ': ' + str(int(r[2]*100)) + 'c -> ' + str(int(r[3]*100)) + 'c  (' + ('+' if move>0 else '') + str(int(move*100)) + 'c)')

# Individual bracket alerts
for _, event_date, label, old_p, new_p, move, model_prob, edge in alerts:
    direction = 'up' if move > 0 else 'down'
    lines.append('')
    lines.append(event_date + '  ' + label)
    lines.append('  ' + str(int(old_p*100)) + 'c -> ' + str(int(new_p*100)) + 'c  (' + ('+' if move>0 else '') + str(int(move*100)) + 'c ' + direction + ')')
    if model_prob is not None and edge is not None:
        edge_pct = int(round(edge * 100))
        lines.append('  model ' + str(int(model_prob*100)) + '%  edge ' + ('+' if edge>0 else '') + str(edge_pct) + '%')
        mkt_moved_toward_model = (move > 0 and edge > 0) or (move < 0 and edge < 0)
        if abs(edge) < 0.03:
            note = 'Model and market were already in agreement. Make of that what you will.'
        elif mkt_moved_toward_model:
            note = 'Market moving toward the model. The path clarifies.'
        elif abs(move) >= 0.30:
            note = 'Market moving sharply away from model. Edge widens to ' + ('+' if edge>0 else '') + str(edge_pct) + '%. Either the market knows something NBM does not, or someone is wrong at scale.'
        else:
            note = 'Market moving away from model. Edge now ' + ('+' if edge>0 else '') + str(edge_pct) + '%. Worth watching.'
        lines.append('  ' + note)

requests.post('https://api.telegram.org/bot' + BOT_TOKEN + '/sendMessage',
    json={'chat_id': CHAT_ID, 'text': '\n'.join(lines)})
