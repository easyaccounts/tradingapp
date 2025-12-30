#!/usr/bin/env python3
"""Quick check of bid/ask data in database"""

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

# Check counts by side
cur.execute("""
    SELECT side, COUNT(*), MIN(time), MAX(time)
    FROM depth_levels_200
    WHERE time::date = CURRENT_DATE
    GROUP BY side
""")

print("Side breakdown:")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]} records, {row[2]} to {row[3]}")

# Check a specific timestamp
cur.execute("""
    SELECT time, side, COUNT(*)
    FROM depth_levels_200
    WHERE time >= '2025-12-30 09:35:09'::timestamp
      AND time < '2025-12-30 09:35:10'::timestamp
    GROUP BY time, side
    ORDER BY time
    LIMIT 10
""")

print("\nSample at 09:35:09:")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]} - {row[2]} records")

cur.close()
conn.close()
