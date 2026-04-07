import sqlite3, requests, sys
from datetime import date, datetime, timezone, timedelta
from collections import defaultdict

DB = '/home/ubuntu/nyc-temp-model/nyc_temp_log.sqlite'
BOT_TOKEN = '8652874695:AAFie5ef1mj7YXFeCs1yFiDqOEO4A76Ekg4'
CHAT_ID = -5229782521
NBM_SHIFT_THRESHOLD_F = 2.0   # PROVISIONAL
PRICE_MOVE_THRESHOLD  = 0.15  # PROVISIONAL
SWEEP_THRESHOLD       = 0.10  # PROVISIONAL
SWEEP_MIN_BRACKETS    = 3     # PROVISIONAL
CONVICTION_THRESHOLD  = 0.70  # PROVISIONAL

conn = sqlite3.connect(DB)
today = date.today().isoformat()

rows = conn.execute('''
    SELECT bracket_label, model_prob, market_price, edge, nbm_p50_adj, nbm_p10_adj, nbm_p90_adj
    FROM bracket_snapshots
    WHERE event_date=? AND snapshot_type='morning'
    ORDER BY bracket_lower_f
''', (today,)).fetchall()

if not rows:
    msg = 'Morning edges ' + today + ': no morning snapshot found. Check morning_model.sh cron.'
    requests.post('https://api.telegram.org/bot' + BOT_TOKEN + '/sendMessage',
        json={'chat_id': CHAT_ID, 'text': msg})
    sys.exit(0)

p50 = rows[0][4]
spread = round(rows[0][6] - rows[0][5], 1)
best = max(rows, key=lambda r: r[3])
worst = min(rows, key=lambda r: r[3])

lines = ['Morning edges — ' + today, 'NBM p50: ' + str(round(p50, 1)) + 'F  spread: ' + str(spread) + 'F', '']
for r in rows:
    bar = '[+]' if r[3] > 0 else '[-]'
    lines.append(bar + ' ' + r[0].ljust(10) + ' model ' + str(int(r[1]*100)) + '%  mkt ' + str(int(r[2]*100)) + '%  edge ' + ('+' if r[3]>0 else '') + str(round(r[3]*100, 1)) + '%')

lines.append('')
lines.append('Best:  ' + best[0] + ' ' + ('+' if best[3]>0 else '') + str(round(best[3]*100, 1)) + '%')
lines.append('Worst: ' + worst[0] + ' ' + ('+' if worst[3]>0 else '') + str(round(worst[3]*100, 1)) + '%')

# NBM shift vs previous morning run
prev = conn.execute('''
    SELECT snapshot_ts, nbm_p50_adj FROM bracket_snapshots
    WHERE snapshot_type='morning' AND event_date=? AND snapshot_ts < (
        SELECT MAX(snapshot_ts) FROM bracket_snapshots WHERE snapshot_type='morning' AND event_date=?
    )
    ORDER BY snapshot_ts DESC LIMIT 1
''', (today, today)).fetchone()

# No intraday fallback — shift vs stale open price is meaningless

if prev and prev[1] is not None:
    shift = p50 - prev[1]
    if abs(shift) >= NBM_SHIFT_THRESHOLD_F:
        direction = 'warmer' if shift > 0 else 'colder'
        lines.append('')
        lines.append('NBM shift: ' + str(round(prev[1], 1)) + 'F -> ' + str(round(p50, 1)) + 'F  (' + ('+' if shift>0 else '') + str(round(shift, 1)) + 'F ' + direction + ')')
        if abs(shift) >= 5:
            lines.append('A ' + str(round(abs(shift), 1)) + 'F revision. The spice — and apparently the atmosphere — is in motion.')
        elif abs(shift) >= 3:
            lines.append('Meaningful revision. Market may not have caught up yet.')
        else:
            lines.append('Modest but notable. Worth checking whether bracket prices have adjusted.')

# Overnight digest: midnight->7am ET = 04:00->11:00 UTC
now_utc = datetime.now(timezone.utc)
overnight_start = datetime(now_utc.year, now_utc.month, now_utc.day, 4, 0, tzinfo=timezone.utc)

overnight_ts_rows = conn.execute('''
    SELECT DISTINCT snapshot_ts FROM bracket_snapshots
    WHERE snapshot_type='intraday'
      AND snapshot_ts >= ? AND snapshot_ts <= ?
    ORDER BY snapshot_ts
''', (overnight_start.strftime('%Y-%m-%dT%H:%M:%SZ'), now_utc.strftime('%Y-%m-%dT%H:%M:%SZ'))).fetchall()

overnight_ticks = [r[0] for r in overnight_ts_rows]

overnight_events = []

# Price moves between consecutive overnight ticks
for i in range(1, len(overnight_ticks)):
    ts_old, ts_new = overnight_ticks[i-1], overnight_ticks[i]
    old_rows = {(r[0], r[1]): r for r in conn.execute('''
        SELECT event_date, bracket_label, market_price FROM bracket_snapshots
        WHERE snapshot_type='intraday' AND snapshot_ts=?
    ''', (ts_old,)).fetchall()}
    new_rows = {(r[0], r[1]): r for r in conn.execute('''
        SELECT event_date, bracket_label, market_price FROM bracket_snapshots
        WHERE snapshot_type='intraday' AND snapshot_ts=?
    ''', (ts_new,)).fetchall()}

    moves_by_date = defaultdict(list)
    for key, new in new_rows.items():
        if key not in old_rows:
            continue
        old = old_rows[key]
        if new[2] is None or old[2] is None:
            continue
        move = new[2] - old[2]
        moves_by_date[key[0]].append((key[1], old[2], new[2], move))

    for event_date, moves in moves_by_date.items():
        # Sweep check
        large = [m for m in moves if abs(m[3]) >= SWEEP_THRESHOLD]
        if len(large) >= SWEEP_MIN_BRACKETS:
            overnight_events.append(
                'SWEEP ' + ts_new[:16] + 'Z  ' + event_date + ': ' +
                str(len(large)) + ' brackets moved \u226510c  (' +
                ', '.join(m[0] + ('+' if m[3]>0 else '') + str(int(m[3]*100)) + 'c' for m in sorted(large, key=lambda x: x[0])) + ')'
            )
        else:
            # Individual large moves
            for label, old_p, new_p, move in moves:
                if abs(move) >= PRICE_MOVE_THRESHOLD:
                    overnight_events.append(
                        ts_new[:16] + 'Z  ' + event_date + '  ' + label + ': ' +
                        str(int(old_p*100)) + 'c -> ' + str(int(new_p*100)) + 'c  (' +
                        ('+' if move>0 else '') + str(int(move*100)) + 'c)'
                    )

# Conviction crossings overnight
for event_date_row in conn.execute(
    'SELECT DISTINCT event_date FROM bracket_snapshots WHERE snapshot_type="intraday"'
).fetchall():
    event_date = event_date_row[0]
    for label_row in conn.execute(
        'SELECT DISTINCT bracket_label FROM bracket_snapshots WHERE snapshot_type="intraday" AND event_date=?',
        (event_date,)
    ).fetchall():
        label = label_row[0]
        # First crossing >= CONVICTION_THRESHOLD for this event_date/bracket
        first_cross = conn.execute('''
            SELECT MIN(snapshot_ts) FROM bracket_snapshots
            WHERE snapshot_type='intraday' AND event_date=? AND bracket_label=?
              AND market_price >= ?
        ''', (event_date, label, CONVICTION_THRESHOLD)).fetchone()[0]
        if first_cross and overnight_start.strftime('%Y-%m-%dT%H:%M:%SZ') <= first_cross <= now_utc.strftime('%Y-%m-%dT%H:%M:%SZ'):
            price = conn.execute('''
                SELECT market_price FROM bracket_snapshots
                WHERE snapshot_type='intraday' AND event_date=? AND bracket_label=? AND snapshot_ts=?
            ''', (event_date, label, first_cross)).fetchone()[0]
            overnight_events.append(
                first_cross[:16] + 'Z  ' + event_date + '  ' + label +
                ' crossed 70% for first time (' + str(int(round(price*100))) + 'c)'
            )

if overnight_events:
    lines.append('')
    lines.append('Overnight (' + overnight_start.strftime('%H:%M') + '-' + now_utc.strftime('%H:%M') + ' UTC):')
    for e in overnight_events:
        lines.append('  ' + e)
else:
    lines.append('')
    lines.append('Overnight: quiet.')

requests.post('https://api.telegram.org/bot' + BOT_TOKEN + '/sendMessage',
    json={'chat_id': CHAT_ID, 'text': '\n'.join(lines)})
