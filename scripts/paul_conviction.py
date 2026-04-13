import sqlite3, requests, sys

DB = '/home/ubuntu/nyc-temp-model/nyc_temp_log.sqlite'
BOT_TOKEN = '8652874695:AAFie5ef1mj7YXFeCs1yFiDqOEO4A76Ekg4'
CHAT_ID = -5229782521
CONVICTION_THRESHOLD = 0.70  # PROVISIONAL

conn = sqlite3.connect(DB)

# Latest intraday snapshot_ts (NYC only)
latest = conn.execute('''
    SELECT DISTINCT snapshot_ts FROM bracket_snapshots
    WHERE snapshot_type='intraday'
      AND COALESCE(series_ticker,'KXHIGHNY')='KXHIGHNY'
    ORDER BY snapshot_ts DESC LIMIT 1
''').fetchone()

if not latest:
    sys.exit(0)

ts_now = latest[0]

# All NYC brackets in the latest tick at or above threshold
new_rows = conn.execute('''
    SELECT event_date, bracket_label, market_price
    FROM bracket_snapshots
    WHERE snapshot_type='intraday' AND snapshot_ts=? AND market_price >= ?
      AND COALESCE(series_ticker,'KXHIGHNY')='KXHIGHNY'
''', (ts_now, CONVICTION_THRESHOLD)).fetchall()

if not new_rows:
    sys.exit(0)

alerts = []
for event_date, label, price in new_rows:
    # Check if any earlier snapshot for this event_date/bracket was already >= threshold
    prior_high = conn.execute('''
        SELECT MAX(market_price) FROM bracket_snapshots
        WHERE snapshot_type='intraday'
          AND event_date=? AND bracket_label=?
          AND snapshot_ts < ?
          AND COALESCE(series_ticker,'KXHIGHNY')='KXHIGHNY'
    ''', (event_date, label, ts_now)).fetchone()[0]

    if prior_high is None or prior_high < CONVICTION_THRESHOLD:
        alerts.append((event_date, label, price))

if not alerts:
    sys.exit(0)

lines = ['Market conviction — ' + ts_now]
for event_date, label, price in alerts:
    lines.append('')
    lines.append(event_date + '  ' + label + '  ' + str(int(round(price * 100))) + 'c')
    lines.append('  First time this bracket has crossed 70% today. The market has made up its mind.')

requests.post('https://api.telegram.org/bot' + BOT_TOKEN + '/sendMessage',
    json={'chat_id': CHAT_ID, 'text': '\n'.join(lines)})
