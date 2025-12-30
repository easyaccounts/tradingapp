"""
Analyze Depth Data Clusters
Finds significant price levels with order concentrations from today's depth data
"""

import psycopg2
import os
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

# Database connection
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 6432)),  # PgBouncer port for host
    'database': os.getenv('DB_NAME', 'tradingdb'),
    'user': os.getenv('DB_USER', 'tradinguser'),
    'password': os.getenv('DB_PASSWORD', 'tradingpass')
}

def connect_db():
    """Connect to database"""
    return psycopg2.connect(**DB_CONFIG)

def get_todays_data(conn):
    """Fetch all depth data from today"""
    cursor = conn.cursor()
    
    # Get data from today (UTC)
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    
    query = """
    SELECT 
        time,
        security_id,
        side,
        level_num,
        price,
        quantity,
        orders
    FROM depth_levels_200
    WHERE time >= %s
    ORDER BY time, side, level_num
    """
    
    cursor.execute(query, (today_start,))
    rows = cursor.fetchall()
    cursor.close()
    
    print(f"✓ Fetched {len(rows):,} depth level records from today")
    return rows

def analyze_price_clusters(rows, min_orders=20):
    """
    Analyze price levels to find clusters of orders
    
    Returns:
    - Top price levels by order concentration
    - Average orders per level
    - Standard deviation
    """
    # Group by price and aggregate
    price_data = defaultdict(lambda: {'orders': [], 'quantity': [], 'count': 0, 'side': None})
    
    for row in rows:
        time, security_id, side, level_num, price, quantity, orders = row
        
        # Round price to nearest 0.5 to group similar levels
        price_rounded = round(price * 2) / 2
        
        price_data[price_rounded]['orders'].append(orders)
        price_data[price_rounded]['quantity'].append(quantity)
        price_data[price_rounded]['count'] += 1
        
        # Track dominant side at this price
        if price_data[price_rounded]['side'] is None:
            price_data[price_rounded]['side'] = side
    
    # Calculate statistics for each price level
    clusters = []
    all_orders = []
    
    for price, data in price_data.items():
        avg_orders = sum(data['orders']) / len(data['orders'])
        max_orders = max(data['orders'])
        avg_quantity = sum(data['quantity']) / len(data['quantity'])
        appearances = data['count']
        
        all_orders.extend(data['orders'])
        
        if avg_orders >= min_orders:
            clusters.append({
                'price': price,
                'avg_orders': avg_orders,
                'max_orders': max_orders,
                'avg_quantity': avg_quantity,
                'appearances': appearances,
                'side': data['side']
            })
    
    # Sort by average orders (strongest clusters first)
    clusters.sort(key=lambda x: x['avg_orders'], reverse=True)
    
    # Calculate global statistics
    if all_orders:
        import statistics
        avg_all = statistics.mean(all_orders)
        stdev_all = statistics.stdev(all_orders) if len(all_orders) > 1 else 0
    else:
        avg_all = 0
        stdev_all = 0
    
    return clusters, avg_all, stdev_all

def get_time_range(conn):
    """Get the time range of data"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT MIN(time), MAX(time), COUNT(DISTINCT time) 
        FROM depth_levels_200 
        WHERE time >= CURRENT_DATE
    """)
    min_time, max_time, snapshot_count = cursor.fetchone()
    cursor.close()
    return min_time, max_time, snapshot_count

def print_clusters(clusters, avg_all, stdev_all, top_n=20):
    """Print top clusters"""
    print("\n" + "="*80)
    print("DEPTH CLUSTER ANALYSIS - TODAY'S DATA")
    print("="*80)
    print(f"\nGlobal Statistics:")
    print(f"  Average orders per level: {avg_all:.1f}")
    print(f"  Standard deviation: {stdev_all:.1f}")
    print(f"  Significance threshold (2.5x avg): {avg_all * 2.5:.1f} orders")
    
    print(f"\n\nTOP {top_n} PRICE CLUSTERS (by order concentration):")
    print("-"*80)
    print(f"{'Price':>10} | {'Side':^8} | {'Avg Orders':>11} | {'Max Orders':>11} | {'Avg Qty':>10} | {'Times Seen':>11}")
    print("-"*80)
    
    for i, cluster in enumerate(clusters[:top_n], 1):
        strength = cluster['avg_orders'] / avg_all if avg_all > 0 else 0
        side_label = 'BID' if cluster['side'] == 'bid' else 'ASK'
        
        print(f"₹{cluster['price']:>8.2f} | {side_label:^8} | "
              f"{cluster['avg_orders']:>11.1f} | "
              f"{cluster['max_orders']:>11} | "
              f"{cluster['avg_quantity']:>10,.0f} | "
              f"{cluster['appearances']:>11,}")
        
        if i == 1:
            print(f"           {'':^8}   ^ STRONGEST CLUSTER ({strength:.1f}x average)")
    
    print("-"*80)

def analyze_support_resistance(clusters, current_price=None):
    """Identify support and resistance levels"""
    if not current_price:
        # Get latest price from data
        return
    
    supports = [c for c in clusters if c['price'] < current_price][:5]
    resistances = [c for c in clusters if c['price'] > current_price][:5]
    
    print("\n" + "="*80)
    print("KEY LEVELS NEAR CURRENT PRICE")
    print("="*80)
    
    if supports:
        print("\nSUPPORT LEVELS (below price):")
        for s in supports:
            distance = current_price - s['price']
            print(f"  ₹{s['price']:.2f} - {s['avg_orders']:.0f} orders avg (−₹{distance:.2f})")
    
    if resistances:
        print("\nRESISTANCE LEVELS (above price):")
        for r in resistances:
            distance = r['price'] - current_price
            print(f"  ₹{r['price']:.2f} - {r['avg_orders']:.0f} orders avg (+₹{distance:.2f})")
    
    print("="*80)

def main():
    """Main analysis"""
    print("Connecting to database...")
    conn = connect_db()
    
    try:
        # Get time range
        min_time, max_time, snapshot_count = get_time_range(conn)
        if min_time and max_time:
            duration = (max_time - min_time).total_seconds() / 60
            print(f"✓ Data range: {min_time.strftime('%Y-%m-%d %H:%M')} to {max_time.strftime('%H:%M')} UTC")
            print(f"✓ Duration: {duration:.1f} minutes ({snapshot_count:,} unique snapshots)")
        
        # Fetch data
        rows = get_todays_data(conn)
        
        if not rows:
            print("✗ No data found for today")
            return
        
        # Analyze clusters
        print("\nAnalyzing price clusters...")
        clusters, avg_all, stdev_all = analyze_price_clusters(rows, min_orders=20)
        
        # Print results
        print_clusters(clusters, avg_all, stdev_all, top_n=25)
        
        # Get current price from latest snapshot
        cursor = conn.cursor()
        cursor.execute("""
            SELECT (best_bid + best_ask) / 2.0 as mid_price
            FROM depth_200_snapshots
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        result = cursor.fetchone()
        if result:
            current_price = result[0]
            print(f"\nCurrent Price: ₹{current_price:.2f}")
            analyze_support_resistance(clusters, current_price)
        
        cursor.close()
        
    finally:
        conn.close()
        print("\n✓ Analysis complete")

if __name__ == '__main__':
    main()
