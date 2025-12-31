#!/usr/bin/env python3
"""
Analyze full depth data for today (security 49229) and identify outliers by bid/ask
Outliers = price levels with unusual quantity or order count patterns
"""

import os
import psycopg2
from datetime import datetime, date
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()


def percentile(data, p):
    """Calculate percentile"""
    if not data:
        return 0
    sorted_data = sorted(data)
    index = int(len(sorted_data) * p / 100)
    return sorted_data[min(index, len(sorted_data) - 1)]


def get_db_connection():
    """Connect to PostgreSQL database"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', 6432)),
        database=os.getenv('DB_NAME', 'tradingdb'),
        user=os.getenv('DB_USER', 'tradinguser'),
        password=os.getenv('DB_PASSWORD')
    )


def analyze_depth_outliers(security_id=49229):
    """Analyze full depth data and identify outliers"""
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print(f"\n{'='*80}")
    print(f"DEPTH OUTLIER ANALYSIS - Security ID: {security_id}")
    print(f"Date: {date.today()} (IST)")
    print(f"{'='*80}\n")
    
    # Load all depth data for today
    query = """
        SELECT 
            side,
            price,
            quantity,
            orders,
            time AT TIME ZONE 'Asia/Kolkata' as time_ist
        FROM depth_levels_200
        WHERE security_id = %s
            AND DATE(time AT TIME ZONE 'Asia/Kolkata') = %s
            AND orders > 0
        ORDER BY side, price DESC, time DESC
    """
    
    cursor.execute(query, (security_id, date.today()))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if not rows:
        print("No depth data found for today")
        return
    
    # Organize data by side and price level
    bid_levels = defaultdict(list)
    ask_levels = defaultdict(list)
    
    for side, price, qty, order_count, time_ist in rows:
        if side == 'BID':
            bid_levels[price].append({
                'qty': qty,
                'orders': order_count,
                'time': time_ist,
                'ratio': qty / order_count if order_count > 0 else 0
            })
        else:
            ask_levels[price].append({
                'qty': qty,
                'orders': order_count,
                'time': time_ist,
                'ratio': qty / order_count if order_count > 0 else 0
            })
    
    # Calculate statistics for each side
    print("BID SIDE ANALYSIS")
    print("-" * 80)
    analyze_side_outliers(bid_levels, 'BID')
    
    print("\n\nASK SIDE ANALYSIS")
    print("-" * 80)
    analyze_side_outliers(ask_levels, 'ASK')


def analyze_side_outliers(levels_dict, side_name):
    """Analyze outliers for one side (BID or ASK) - Qty >= 20x average is outlier"""
    
    # Aggregate all quantities
    all_quantities = []
    
    for price, snapshots in levels_dict.items():
        for snap in snapshots:
            all_quantities.append(snap['qty'])
    
    if not all_quantities:
        print(f"No data for {side_name} side")
        return
    
    # Calculate average quantity
    avg_qty = sum(all_quantities) / len(all_quantities)
    outlier_threshold = avg_qty * 20
    
    print(f"\nStatistics ({len(all_quantities)} total snapshots):")
    print(f"  Average Qty:         {avg_qty:,.0f}")
    print(f"  Outlier Threshold:   {outlier_threshold:,.0f} (20x avg)")
    
    # Collect only outliers (qty >= 20x average)
    outliers = []
    
    for price, snapshots in sorted(levels_dict.items(), reverse=True):
        for snap in snapshots:
            if snap['qty'] >= outlier_threshold:
                outliers.append({
                    'price': price,
                    'qty': snap['qty'],
                    'orders': snap['orders'],
                    'ratio': snap['ratio'],
                    'time': snap['time'],
                    'multiplier': snap['qty'] / avg_qty
                })
    
    # Display only top 10 outliers by multiplier
    if outliers:
        top_outliers = sorted(outliers, key=lambda x: x['multiplier'], reverse=True)[:10]
        print(f"\n🔴 TOP 10 OUTLIERS (by multiplier) - Total detected: {len(outliers)}\n")
        print(f"{'Price':<12} {'Qty':<12} {'Orders':<10} {'Qty/Order':<12} {'Multiplier':<12} {'Time':<20}")
        print("-" * 98)
        
        for outlier in top_outliers:
            print(f"{outlier['price']:<12.2f} {outlier['qty']:<12,} {outlier['orders']:<10} "
                  f"{outlier['ratio']:<12,.1f} {outlier['multiplier']:<12.1f}x {str(outlier['time']):<20}")
    else:
        print(f"\n✓ No outliers detected on {side_name} side (Qty < 20x average)")


if __name__ == "__main__":
    analyze_depth_outliers(security_id=49229)