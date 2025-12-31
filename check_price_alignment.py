"""Check if tick and depth data have matching prices for the same time window"""
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST', 'localhost'),
    port=os.getenv('DB_PORT', '6432'),
    database=os.getenv('DB_NAME', 'tradingdb'),
    user=os.getenv('DB_USER', 'tradinguser'),
    password=os.getenv('DB_PASSWORD')
)

cur = conn.cursor()

# 9:30 AM to 9:35 AM IST = 4:00 AM to 4:05 AM UTC
start = '2025-12-31 04:00:00+00:00'
end = '2025-12-31 04:05:00+00:00'

print("=" * 80)
print("MARKET HOURS CHECK")
print("=" * 80)
print("IST Market Hours: 9:15 AM - 3:30 PM")
print("UTC Market Hours: 3:45 AM - 10:00 AM")
print(f"\nQuery window: {start} to {end}")
print(f"In IST: 9:30 AM to 9:35 AM (market open)")

# Get actual market open times
cur.execute("""
    SELECT MIN(time), MAX(time)
    FROM ticks
    WHERE instrument_token = 12602626
    AND DATE(time) = '2025-12-31'
""", ())
tick_range = cur.fetchone()

cur.execute("""
    SELECT MIN(time), MAX(time)
    FROM depth_levels_200
    WHERE security_id = 49229
    AND DATE(time) = '2025-12-31'
""", ())
depth_range = cur.fetchone()

print(f"\nFull day data ranges on 2025-12-31:")
print(f"  Ticks:  {tick_range[0]} to {tick_range[1]}")
print(f"  Depth:  {depth_range[0]} to {depth_range[1]}")

print("\n" + "=" * 80)
print("TICKS TABLE (Kite feed - token 12602626)")
print("=" * 80)

cur.execute("""
    SELECT time, last_price, volume_delta
    FROM ticks
    WHERE instrument_token = 12602626
    AND time >= %s AND time < %s
    ORDER BY time
    LIMIT 20
""", (start, end))

print(f"\nFirst 20 ticks from {start} to {end}:")
for row in cur.fetchall():
    print(f"  {row[0]} | Price: ₹{row[1]:.2f} | Volume: {row[2]}")

cur.execute("""
    SELECT 
        MIN(last_price) as min_price,
        MAX(last_price) as max_price,
        COUNT(*) as tick_count
    FROM ticks
    WHERE instrument_token = 12602626
    AND time >= %s AND time < %s
""", (start, end))

row = cur.fetchone()
print(f"\nSummary: {row[2]} ticks, Price range: ₹{row[0]:.2f} - ₹{row[1]:.2f}")

print("\n" + "=" * 80)
print("DEPTH_LEVELS_200 TABLE (Dhan feed - security_id 49229)")
print("=" * 80)

cur.execute("""
    SELECT time, side, level_num, price, quantity
    FROM depth_levels_200
    WHERE security_id = 49229
    AND time >= %s AND time < %s
    ORDER BY time, side DESC, level_num
    LIMIT 20
""", (start, end))

print(f"\nFirst 20 depth levels from {start} to {end}:")
for row in cur.fetchall():
    print(f"  {row[0]} | {row[1]:3s} L{row[2]:3d} | Price: ₹{row[3]:.2f} | Qty: {row[4]}")

cur.execute("""
    SELECT 
        MIN(price) as min_price,
        MAX(price) as max_price,
        COUNT(*) as level_count,
        COUNT(DISTINCT time) as snapshot_count
    FROM depth_levels_200
    WHERE security_id = 49229
    AND time >= %s AND time < %s
""", (start, end))

row = cur.fetchone()
print(f"\nSummary: {row[2]} depth levels across {row[3]} snapshots")
print(f"Price range: ₹{row[0]:.2f} - ₹{row[1]:.2f}")

print("\n" + "=" * 80)
print("COMPARISON")
print("=" * 80)
print("\nIf prices are similar (within ±5), they're the same instrument.")
print("If prices differ by >100, they're different contracts (different expiry).")

cur.close()
conn.close()
