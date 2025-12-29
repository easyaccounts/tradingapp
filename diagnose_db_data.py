import psycopg2
from datetime import datetime
import pytz

# Database connection
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="tradingdb",
    user="tradinguser",
    password="5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ="
)

cursor = conn.cursor()

print("=" * 80)
print("DIAGNOSTIC: Checking NIFTY Futures data availability")
print("=" * 80)

# Check if instrument exists
cursor.execute("""
    SELECT COUNT(*), MIN(time), MAX(time) 
    FROM ticks 
    WHERE instrument_token = 12683010
""")
result = cursor.fetchone()
print(f"\nInstrument 12683010 (NIFTY Futures):")
print(f"  Total ticks: {result[0]:,}")
if result[0] > 0:
    print(f"  First tick: {result[1]}")
    print(f"  Last tick:  {result[2]}")

# Check today's data
cursor.execute("""
    SELECT COUNT(*), MIN(time), MAX(time) 
    FROM ticks 
    WHERE instrument_token = 12683010
      AND time::date = '2025-12-29'
""")
result = cursor.fetchone()
print(f"\nToday (2025-12-29):")
print(f"  Total ticks: {result[0]:,}")
if result[0] > 0:
    print(f"  First tick: {result[1]}")
    print(f"  Last tick:  {result[2]}")

# Check around 12:47 IST
cursor.execute("""
    SELECT time, last_price, volume_traded 
    FROM ticks 
    WHERE instrument_token = 12683010
      AND time >= '2025-12-29 12:45:00'
      AND time <= '2025-12-29 12:55:00'
    ORDER BY time 
    LIMIT 10
""")
rows = cursor.fetchall()
print(f"\nSample ticks near 12:47:00 (found {len(rows)}):")
for row in rows:
    print(f"  {row[0]} - LTP: ₹{row[1]:,.2f}, Volume: {row[2]:,}")

# Check if timezone is the issue - try UTC conversion
cursor.execute("""
    SELECT time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata' as ist_time, 
           last_price, volume_traded 
    FROM ticks 
    WHERE instrument_token = 12683010
      AND time >= '2025-12-29 12:45:00+05:30'
      AND time <= '2025-12-29 12:55:00+05:30'
    ORDER BY time 
    LIMIT 10
""")
rows = cursor.fetchall()
print(f"\nTrying with explicit timezone (found {len(rows)}):")
for row in rows:
    print(f"  {row[0]} - LTP: ₹{row[1]:,.2f}, Volume: {row[2]:,}")

cursor.close()
conn.close()

print("\n" + "=" * 80)
