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
    """Analyze outliers for one side (BID or ASK) using IQR method"""
    
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
    
    # Calculate IQR-based thresholds (more robust than std dev)
    q1_qty = percentile(all_quantities, 25)
    q3_qty = percentile(all_quantities, 75)
    iqr_qty = q3_qty - q1_qty
    
    q1_orders = percentile(all_orders, 25)
    q3_orders = percentile(all_orders, 75)
    iqr_orders = q3_orders - q1_orders
    
    q1_ratio = percentile(all_ratios, 25)
    q3_ratio = percentile(all_ratios, 75)
    iqr_ratio = q3_ratio - q1_ratio
    
    # IQR outlier bounds (1.5x IQR is standard)
    qty_lower = q1_qty - (1.5 * iqr_qty)
    qty_upper = q3_qty + (1.5 * iqr_qty)
    
    orders_lower = q1_orders - (1.5 * iqr_orders)
    orders_upper = q3_orders + (1.5 * iqr_orders)
    
    ratio_lower = q1_ratio - (1.5 * iqr_ratio)
    ratio_upper = q3_ratio + (1.5 * iqr_ratio)
    
    print(f"\nStatistics ({len(all_quantities)} total snapshots):")
    print(f"  Quantity:  Q1={percentile(all_quantities, 25):,.0f}  Q3={percentile(all_quantities, 75):,.0f}  IQR={iqr_qty:,.0f}")
    print(f"  Orders:    Q1={percentile(all_orders, 25):.0f}  Q3={percentile(all_orders, 75):.0f}  IQR={iqr_orders:.0f}")
    print(f"  Qty/Order: Q1={percentile(all_ratios, 25):,.1f}  Q3={percentile(all_ratios, 75):,.1f}  IQR={iqr_ratio:,.1f}")
    
    # Collect outliers only
    outliers = []
    
    for price, snapshots in sorted(levels_dict.items(), reverse=True):
        for snap in snapshots:
            is_outlier = False
            reasons = []
            
            if snap['qty'] > qty_upper:
                is_outlier = True
                reasons.append(f"High Qty ({snap['qty']:,})")
            elif snap['qty'] < qty_lower:
                is_outlier = True
                reasons.append(f"Low Qty ({snap['qty']:,})")
            
            if snap['orders'] > orders_upper:
                is_outlier = True
                reasons.append(f"High Orders ({snap['orders']})")
            elif snap['orders'] < orders_lower:
                is_outlier = True
                reasons.append(f"Low Orders ({snap['orders']})")
            
            if snap['ratio'] > ratio_upper:
                is_outlier = True
                reasons.append(f"High Ratio ({snap['ratio']:,.1f})")
            elif snap['ratio'] < ratio_lower:
                is_outlier = True
                reasons.append(f"Low Ratio ({snap['ratio']:,.1f})")
            
            if is_outlier:
                outliers.append({
                    'price': price,
                    'qty': snap['qty'],
                    'orders': snap['orders'],
                    'ratio': snap['ratio'],
                    'time': snap['time'],
                    'reasons': reasons
                })
    
    # Display only outliers
    if outliers:
        print(f"\n🔴 OUTLIERS DETECTED ({len(outliers)} instances):\n")
        print(f"{'Price':<12} {'Qty':<12} {'Orders':<10} {'Qty/Order':<12} {'Time':<20} {'Reason':<50}")
        print("-" * 116)
        
        for outlier in sorted(outliers, key=lambda x: x['price'], reverse=True):
            reasons_str = " | ".join(outlier['reasons'])[:49]
            print(f"{outlier['price']:<12.2f} {outlier['qty']:<12,} {outlier['orders']:<10} "
                  f"{outlier['ratio']:<12,.1f} {str(outlier['time']):<20} {reasons_str:<50}")
    else:
        print(f"\n✓ No outliers detected on {side_name} side")


if __name__ == "__main__":
    analyze_depth_outliers(security_id=49229)