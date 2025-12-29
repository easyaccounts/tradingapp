"""
Quick check of NIFTY_FUT data in database
"""
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

db_host = 'localhost'
db_port = os.getenv('DB_PORT', '5432')
db_name = os.getenv('DB_NAME', 'tradingdb')
db_user = os.getenv('DB_USER', 'tradinguser')
db_password = os.getenv('DB_PASSWORD')

conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    database=db_name,
    user=db_user,
    password=db_password
)

cur = conn.cursor()

# Check total candles and date range
cur.execute("SELECT COUNT(*), MIN(time), MAX(time) FROM historical_data WHERE trading_symbol='NIFTY_FUT' AND timeframe='day'")
result = cur.fetchone()
if result:
    count, min_date, max_date = result
    print(f'Total candles in DB: {count}')
    print(f'Date range: {min_date} to {max_date}')
else:
    print('No data found')
    conn.close()
    exit(1)

# Check for any volume=0 or NULL volume
cur.execute("SELECT COUNT(*) FROM historical_data WHERE trading_symbol='NIFTY_FUT' AND timeframe='day' AND (volume IS NULL OR volume = 0)")
result = cur.fetchone()
zero_vol = result[0] if result else 0
print(f'Candles with zero/null volume: {zero_vol}')

# Sample some data
cur.execute("SELECT time, volume FROM historical_data WHERE trading_symbol='NIFTY_FUT' AND timeframe='day' ORDER BY time DESC LIMIT 10")
print(f'\nLatest 10 candles:')
for row in cur.fetchall():
    print(f'  {row[0]}: volume={row[1]:,}')

conn.close()
