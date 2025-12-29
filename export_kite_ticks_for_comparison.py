import psycopg2
import csv
from datetime import datetime
import pytz

# Database connection (running from VPS host, not container)
# pgbouncer is only accessible from inside Docker network, so we use localhost
# TimescaleDB should be exposed on port 5432 on the host
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="tradingdb",
    user="tradinguser",
    password="5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ="
)

cursor = conn.cursor()

# Time range from Dhan data (converting IST to UTC)
# Database stores in UTC, so 12:47 IST = 07:17 UTC (IST is UTC+5:30)
start_time = "2025-12-29 07:17:00+00:00"  # 12:47 IST
end_time = "2025-12-29 07:22:00+00:00"    # 12:52 IST

# Query for NIFTY Futures (instrument_token 12683010)
query = """
SELECT 
    time,
    instrument_token,
    trading_symbol,
    last_price,
    volume_traded,
    oi,
    last_traded_quantity,
    average_traded_price,
    total_buy_quantity,
    total_sell_quantity,
    day_open,
    day_high,
    day_low,
    day_close,
    change,
    last_trade_time,
    oi_delta,
    volume_delta,
    aggressor_side,
    cvd_change,
    depth_toxicity_tick,
    kyle_lambda_tick,
    bid_prices,
    bid_quantities,
    bid_orders,
    ask_prices,
    ask_quantities,
    ask_orders
FROM ticks
WHERE instrument_token = 12683010
  AND time >= %s
  AND time <= %s
ORDER BY time ASC
"""

print(f"Extracting KiteConnect data from {start_time} (UTC) = 12:47-12:52 IST")
print(f"Instrument: NIFTY Futures (token 12683010)")
print(f"=" * 80)

cursor.execute(query, (start_time, end_time))
rows = cursor.fetchall()

print(f"Found {len(rows)} ticks")

# Write to CSV
output_file = "kiteconnect_nifty_futures_ticks.csv"

with open(output_file, 'w', newline='') as f:
    writer = csv.writer(f)
    
    # Headers
    headers = [
        'time', 'instrument_token', 'trading_symbol', 'last_price', 'volume_traded',
        'oi', 'last_traded_quantity', 'average_traded_price', 'total_buy_quantity', 'total_sell_quantity',
        'day_open', 'day_high', 'day_low', 'day_close', 'change', 'last_trade_time',
        'oi_delta', 'volume_delta', 'aggressor_side', 'cvd_change',
        'depth_toxicity_tick', 'kyle_lambda_tick',
        'bid_price_1', 'bid_price_2', 'bid_price_3', 'bid_price_4', 'bid_price_5',
        'bid_qty_1', 'bid_qty_2', 'bid_qty_3', 'bid_qty_4', 'bid_qty_5',
        'bid_orders_1', 'bid_orders_2', 'bid_orders_3', 'bid_orders_4', 'bid_orders_5',
        'ask_price_1', 'ask_price_2', 'ask_price_3', 'ask_price_4', 'ask_price_5',
        'ask_qty_1', 'ask_qty_2', 'ask_qty_3', 'ask_qty_4', 'ask_qty_5',
        'ask_orders_1', 'ask_orders_2', 'ask_orders_3', 'ask_orders_4', 'ask_orders_5'
    ]
    writer.writerow(headers)
    
    # Data rows
    for row in rows:
        # Unpack arrays
        bid_prices = row[22] if row[22] else [None]*5
        bid_quantities = row[23] if row[23] else [None]*5
        bid_orders = row[24] if row[24] else [None]*5
        ask_prices = row[25] if row[25] else [None]*5
        ask_quantities = row[26] if row[26] else [None]*5
        ask_orders = row[27] if row[27] else [None]*5
        
        # Ensure we have 5 levels
        bid_prices = (list(bid_prices) + [None]*5)[:5]
        bid_quantities = (list(bid_quantities) + [None]*5)[:5]
        bid_orders = (list(bid_orders) + [None]*5)[:5]
        ask_prices = (list(ask_prices) + [None]*5)[:5]
        ask_quantities = (list(ask_quantities) + [None]*5)[:5]
        ask_orders = (list(ask_orders) + [None]*5)[:5]
        
        csv_row = [
            row[0],  # time
            row[1],  # instrument_token
            row[2],  # trading_symbol
            row[3],  # last_price
            row[4],  # volume_traded
            row[5],  # oi
            row[6],  # last_quantity
            row[7],  # average_price
            row[8],  # total_buy_quantity
            row[9],  # total_sell_quantity
            row[10], # open
            row[11], # high
            row[12], # low
            row[13], # close
            row[14], # change
            row[15], # last_trade_time
            row[16], # oi_delta
            row[17], # volume_delta
            row[18], # aggressor_side
            row[19], # cvd_change
            row[20], # depth_toxicity_tick
            row[21], # kyle_lambda_tick
        ] + bid_prices + bid_quantities + bid_orders + ask_prices + ask_quantities + ask_orders
        
        writer.writerow(csv_row)

print(f"\nData exported to: {output_file}")
print(f"\nSample (first 5 ticks):")
print("=" * 80)

cursor.execute(query + " LIMIT 5", (start_time, end_time))
sample = cursor.fetchall()
for i, row in enumerate(sample, 1):
    print(f"Tick #{i}: Time={row[0]}, LTP=₹{row[3]:,.2f}, Vol={row[4]:,}, OI={row[5]:,}")

cursor.close()
conn.close()

print(f"\n{'=' * 80}")
print(f"Export complete!")
print(f"KiteConnect ticks: {len(rows)}")
print(f"Time range: {start_time} to {end_time} IST")
print(f"File: {output_file}")
print(f"{'=' * 80}")
