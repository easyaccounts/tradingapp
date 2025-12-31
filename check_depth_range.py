#!/usr/bin/env python3
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database=os.getenv('DB_NAME', 'tradingdb'),
    user=os.getenv('DB_USER', 'tradinguser'),
    password=os.getenv('DB_PASSWORD')
)

cur = conn.cursor()

# Get latest snapshot
cur.execute('SELECT MAX(time) FROM depth_levels_200 WHERE time::date = CURRENT_DATE')
latest = cur.fetchone()[0]

# Get bid range (level 1 to 20)
cur.execute('''
    SELECT MIN(price) as lowest_bid, MAX(price) as highest_bid
    FROM depth_levels_200
    WHERE time = %s AND side = 'BID' AND level_num BETWEEN 1 AND 20 AND price > 0
''', (latest,))
bid_low, bid_high = cur.fetchone()

# Get ask range (level 1 to 20)
cur.execute('''
    SELECT MIN(price) as lowest_ask, MAX(price) as highest_ask
    FROM depth_levels_200
    WHERE time = %s AND side = 'ASK' AND level_num BETWEEN 1 AND 20 AND price > 0
''', (latest,))
ask_low, ask_high = cur.fetchone()

print(f'BID SIDE (Level 1-20):')
print(f'  Highest (Level 1): ₹{bid_high:.2f}')
print(f'  Lowest (Level 20): ₹{bid_low:.2f}')
print(f'  Range: {bid_high - bid_low:.2f} points')
print()
print(f'ASK SIDE (Level 1-20):')
print(f'  Lowest (Level 1): ₹{ask_low:.2f}')
print(f'  Highest (Level 20): ₹{ask_high:.2f}')
print(f'  Range: {ask_high - ask_low:.2f} points')
print()
print(f'TOTAL SPREAD (20 levels each side): {ask_high - bid_low:.2f} points')

cur.close()
conn.close()
