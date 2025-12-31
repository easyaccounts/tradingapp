#!/usr/bin/env python3
"""
Analyze full depth data for today (security 49229) and identify outliers by bid/ask
Outliers = price levels with unusual quantity or order count patterns
"""

import os
import psycopg2
from datetime import datetime, date
from collections import defaultdict
from statistics import mean, stdev
from dotenv import load_dotenv

load_dotenv()


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
    """Analyze outliers for one side (BID or ASK)"""
    
    # Aggregate statistics across all price levels
    all_quantities = []
    all_orders = []
    all_ratios = []
    
    for price, snapshots in levels_dict.items():
        for snap in snapshots:
            all_quantities.append(snap['qty'])
            all_orders.append(snap['orders'])
            all_ratios.append(snap['ratio'])
    
    if not all_quantities:
        print(f"No data for {side_name} side")
        return
    
    # Calculate statistics
    qty_mean = mean(all_quantities)
    qty_stdev = stdev(all_quantities) if len(all_quantities) > 1 else 0
    
    orders_mean = mean(all_orders)
    orders_stdev = stdev(all_orders) if len(all_orders) > 1 else 0
    
    ratio_mean = mean(all_ratios)
    ratio_stdev = stdev(all_ratios) if len(all_ratios) > 1 else 0
    
    print(f"\nStatistics ({len(all_quantities)} total snapshots):")
    print(f"  Quantity:  μ={qty_mean:,.0f}  σ={qty_stdev:,.0f}")
    print(f"  Orders:    μ={orders_mean:.1f}  σ={orders_stdev:.1f}")
    print(f"  Qty/Order: μ={ratio_mean:,.1f}  σ={ratio_stdev:,.1f}")
    
    # Find outliers (>2 sigma)
    qty_threshold_high = qty_mean + (2 * qty_stdev)
    qty_threshold_low = max(0, qty_mean - (2 * qty_stdev))
    
    orders_threshold_high = orders_mean + (2 * orders_stdev)
    orders_threshold_low = max(0, orders_mean - (2 * orders_stdev))
    
    ratio_threshold_high = ratio_mean + (2 * ratio_stdev)
    ratio_threshold_low = max(0, ratio_mean - (2 * ratio_stdev))
    
    # Collect outliers
    outliers = []
    
    for price, snapshots in sorted(levels_dict.items(), reverse=True):
        for snap in snapshots:
            is_outlier = False
            reasons = []
            
            if snap['qty'] > qty_threshold_high:
                is_outlier = True
                reasons.append(f"High Qty ({snap['qty']:,} > {qty_threshold_high:,.0f})")
            elif snap['qty'] < qty_threshold_low:
                is_outlier = True
                reasons.append(f"Low Qty ({snap['qty']:,} < {qty_threshold_low:,.0f})")
            
            if snap['orders'] > orders_threshold_high:
                is_outlier = True
                reasons.append(f"High Orders ({snap['orders']} > {orders_threshold_high:.0f})")
            elif snap['orders'] < orders_threshold_low:
                is_outlier = True
                reasons.append(f"Low Orders ({snap['orders']} < {orders_threshold_low:.0f})")
            
            if snap['ratio'] > ratio_threshold_high:
                is_outlier = True
                reasons.append(f"High Ratio ({snap['ratio']:,.1f} > {ratio_threshold_high:,.1f})")
            elif snap['ratio'] < ratio_threshold_low:
                is_outlier = True
                reasons.append(f"Low Ratio ({snap['ratio']:,.1f} < {ratio_threshold_low:,.1f})")
            
            if is_outlier:
                outliers.append({
                    'price': price,
                    'qty': snap['qty'],
                    'orders': snap['orders'],
                    'ratio': snap['ratio'],
                    'time': snap['time'],
                    'reasons': reasons
                })
    
    # Display outliers sorted by price
    if outliers:
        print(f"\n🔴 OUTLIERS DETECTED ({len(outliers)} instances):\n")
        print(f"{'Price':<12} {'Qty':<12} {'Orders':<10} {'Qty/Order':<12} {'Time':<20} {'Reason':<40}")
        print("-" * 106)
        
        for outlier in sorted(outliers, key=lambda x: x['price'], reverse=True):
            reasons_str = " | ".join(outlier['reasons'])[:39]
            print(f"{outlier['price']:<12.2f} {outlier['qty']:<12,} {outlier['orders']:<10} "
                  f"{outlier['ratio']:<12,.1f} {str(outlier['time']):<20} {reasons_str:<40}")
    else:
        print(f"\n✓ No significant outliers detected on {side_name} side")


if __name__ == "__main__":
    analyze_depth_outliers(security_id=49229)