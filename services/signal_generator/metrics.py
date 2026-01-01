"""
Metrics Calculation Module
Implements the 3 core trading metrics
"""

from collections import deque
from datetime import datetime
from typing import List, Dict
import statistics


def identify_key_levels(current_snapshot: dict, level_tracker, current_price: float) -> List[Dict]:
    """
    Metric 1: Identify and track significant order concentrations
    Returns top 5 verified key levels
    """
    bid_levels = current_snapshot['bids']
    ask_levels = current_snapshot['asks']
    timestamp = datetime.fromisoformat(current_snapshot['timestamp'])  # Parse datetime from ISO string
    
    # Calculate baseline (average orders per level)
    all_orders = [lvl['orders'] for lvl in bid_levels + ask_levels if lvl['orders'] > 0]
    if not all_orders:
        return []
    
    avg_orders = statistics.mean(all_orders)
    threshold = avg_orders * 2.5  # Significant if 2.5x average
    
    current_big_levels = {}
    
    # Scan for big levels within ±100 points
    for level in bid_levels + ask_levels:
        price = level['price']
        orders = level['orders']
        
        if abs(price - current_price) > 100:
            continue
        
        if orders > threshold:
            side = 'support' if price < current_price else 'resistance'
            current_big_levels[price] = {
                'price': price,
                'orders': orders,
                'quantity': level['quantity'],
                'side': side,
                'strength': orders / avg_orders
            }
    
    # Update tracked levels
    for price, data in current_big_levels.items():
        if level_tracker.get_level(price):
            # Existing level - update
            level_tracker.update_level(price, data['orders'], data['quantity'], current_price, timestamp)
        else:
            # New level - start tracking
            level_tracker.add_level(price, data['side'], data['orders'], data['quantity'], timestamp)
    
    # Clean up stale levels
    level_tracker.cleanup_stale_levels(current_price, timestamp)
    
    # Return only verified levels (existed for 5+ seconds)
    verified_levels = [
        {
            'price': lvl.price,
            'side': lvl.side,
            'orders': lvl.current_orders,
            'peak_quantity': lvl.peak_quantity,
            'avg_quantity': lvl.avg_quantity,
            'strength': lvl.current_orders / avg_orders,
            'age_seconds': lvl.age_seconds,
            'age_display': lvl.age_display,
            'tests': lvl.tests,
            'status': lvl.status,
            'distance': abs(lvl.price - current_price)
        }
        for lvl in level_tracker.get_all_levels()
        if lvl.age_seconds > 5  # At least 5 seconds old
    ]
    
    # Sort by strength, return top 5
    verified_levels.sort(key=lambda x: x['strength'], reverse=True)
    return verified_levels[:5]


def detect_absorptions(level_tracker, snapshot_buffer: deque, current_price: float) -> List[Dict]:
    """
    Metric 2: Detect when big levels are being broken through
    Returns list of active absorptions
    """
    if len(snapshot_buffer) < 180:  # Need at least 60 seconds of data
        return []
    
    absorptions = []
    
    # Get snapshots from 30-60 seconds ago
    snapshots_ago = list(snapshot_buffer)[-180:-150] if len(snapshot_buffer) >= 180 else []
    current_snapshots = list(snapshot_buffer)[-15:]  # Last 3 seconds
    
    if not snapshots_ago or not current_snapshots:
        return []

    # Baseline: average resting orders across the older window
    all_orders_ago = [
        lvl['orders']
        for snap in snapshots_ago
        for lvl in snap.get('bids', []) + snap.get('asks', [])
        if lvl['orders'] > 0
    ]
    avg_orders_ago = statistics.mean(all_orders_ago) if all_orders_ago else 0
    if avg_orders_ago == 0:
        return []
    big_level_threshold = avg_orders_ago * 3  # match key-level style (3x average)
    
    for level in level_tracker.get_all_levels():
        price = level.price
        
        # Only check significant levels (3x average)
        if level.peak_orders < big_level_threshold:
            continue
        
        # Get average orders at this price in both windows
        orders_before = get_avg_orders_at_price(snapshots_ago, price)
        orders_now = get_avg_orders_at_price(current_snapshots, price)
        
        if orders_before < big_level_threshold:  # Wasn't big enough back then
            continue
        
        # Calculate reduction
        reduction_pct = (orders_before - orders_now) / orders_before
        
        # Absorption conditions
        if reduction_pct > 0.6:  # 60%+ reduction
            price_distance = abs(current_price - price)
            
            # Price must be near or through the level
            if price_distance < 20:
                # Check if price broke through
                price_broke_through = (
                    (level.side == 'resistance' and current_price > price) or
                    (level.side == 'support' and current_price < price)
                )
                
                # Verify consistent decline (not flickering)
                if level.is_consistent_decline(window=min(30, len(level.order_history))):
                    absorptions.append({
                        'price': price,
                        'side': level.side,
                        'orders_before': int(orders_before),
                        'orders_now': int(orders_now),
                        'reduction_pct': int(reduction_pct * 100),
                        'started_at': level.first_seen.isoformat(),
                        'status': 'breaking' if price_distance < 10 else 'weakening',
                        'price_broke': price_broke_through,
                        'current_price': current_price
                    })
                    
                    # Update level status
                    level.status = 'breaking'
    
    return absorptions


def calculate_pressure(snapshot_buffer: deque, current_price: float) -> Dict:
    """
    Metric 3: Calculate buy/sell pressure at multiple timeframes
    Returns imbalance values and market state
    """
    if len(snapshot_buffer) < 150:  # Need at least 30 seconds
        return {'30s': 0.0, '60s': 0.0, '120s': 0.0, 'state': 'neutral'}
    
    # Calculate for different windows
    imbalance_30s = calc_imbalance_window(
        list(snapshot_buffer)[-150:], 
        current_price, 
        top_n=40
    )
    
    imbalance_60s = calc_imbalance_window(
        list(snapshot_buffer)[-300:] if len(snapshot_buffer) >= 300 else list(snapshot_buffer), 
        current_price, 
        top_n=40
    )
    
    imbalance_120s = calc_imbalance_window(
        list(snapshot_buffer)[-600:] if len(snapshot_buffer) >= 600 else list(snapshot_buffer), 
        current_price, 
        top_n=40
    )
    
    # Determine market state based on primary (60s) window
    primary = imbalance_60s
    if primary > 0.3:
        state = 'bullish'
    elif primary < -0.3:
        state = 'bearish'
    else:
        state = 'neutral'
    
    return {
        '30s': round(imbalance_30s, 3),
        '60s': round(imbalance_60s, 3),
        '120s': round(imbalance_120s, 3),
        'state': state
    }


def calc_imbalance_window(snapshots: List[dict], current_price: float, top_n: int = 40) -> float:
    """Calculate imbalance from top N levels near current price"""
    if not snapshots:
        return 0.0
    
    total_bid_orders = 0
    total_ask_orders = 0
    
    for snapshot in snapshots:
        # Get top N closest levels to current price
        bids_near = get_n_closest_levels(snapshot.get('bids', []), current_price, top_n)
        asks_near = get_n_closest_levels(snapshot.get('asks', []), current_price, top_n)
        
        total_bid_orders += sum(lvl['orders'] for lvl in bids_near)
        total_ask_orders += sum(lvl['orders'] for lvl in asks_near)
    
    if total_bid_orders + total_ask_orders == 0:
        return 0.0
    
    imbalance = (total_bid_orders - total_ask_orders) / (total_bid_orders + total_ask_orders)
    return imbalance


def get_n_closest_levels(levels: List[dict], price: float, n: int) -> List[dict]:
    """Get N levels closest to given price"""
    if not levels:
        return []
    
    sorted_levels = sorted(levels, key=lambda x: abs(x['price'] - price))
    return sorted_levels[:n]


def get_avg_orders_at_price(snapshots: List[dict], price: float, tolerance: float = 2.0) -> float:
    """Get average order count at a specific price level across snapshots"""
    orders_list = []
    
    for snapshot in snapshots:
        # Check both bids and asks
        for level in snapshot.get('bids', []) + snapshot.get('asks', []):
            if abs(level['price'] - price) <= tolerance:
                orders_list.append(level['orders'])
    
    if not orders_list:
        return 0.0
    
    return statistics.mean(orders_list)
