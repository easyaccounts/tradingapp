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
        quantity = level['quantity']
        
        if abs(price - current_price) > 100:
            continue
        
        # Filter: orders > 2.5x average AND quantity > 10k
        if orders > threshold and quantity > 10000:
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
            'distance': abs(lvl.price - current_price),
            'first_seen': lvl.first_seen.strftime('%H:%M:%S') if lvl.first_seen else 'N/A'
        }
        for lvl in level_tracker.get_all_levels()
        if lvl.age_seconds > 5  # At least 5 seconds old
    ]
    
    # Sort by strength, return top 5
    verified_levels.sort(key=lambda x: x['strength'], reverse=True)
    return verified_levels[:5]


def detect_absorptions(key_levels: List[Dict], current_snapshot: dict) -> List[Dict]:
    """
    Metric 2: Detect when key levels are being absorbed/broken through
    Monitors key levels (both orders + qty thresholds met) for absorption
    with natural boundary conditions instead of fixed time windows.
    
    Returns list of confirmed absorptions
    """
    absorptions = []
    
    if not key_levels or not current_snapshot:
        return absorptions
    
    current_price = float(current_snapshot.get('last_price', 0))
    best_bid = float(current_snapshot.get('bids', [{}])[0].get('price', 0)) if current_snapshot.get('bids') else 0
    best_ask = float(current_snapshot.get('asks', [{}])[0].get('price', 0)) if current_snapshot.get('asks') else 0
    timestamp = datetime.fromisoformat(current_snapshot['timestamp'])
    
    for level in key_levels:
        price = level['price']
        side = level['side']
        orders_at_level = level['orders']
        peak_orders = level['peak_quantity']
        
        # Initialize absorption tracking for this key level (first time only)
        if '_absorption_tracking' not in level:
            level['_absorption_tracking'] = {
                'started_at': timestamp,
                'snapshot_count': 0,
                'status': 'monitoring'  # monitoring → done
            }
        
        tracking = level['_absorption_tracking']
        
        # Skip if already processed
        if tracking['status'] == 'done':
            continue
        
        tracking['snapshot_count'] += 1
        time_elapsed = (timestamp - tracking['started_at']).total_seconds()
        distance_from_price = abs(current_price - price)
        
        # === BOUNDARY CONDITION 1: Price moved >40 points away ===
        # Level no longer in play for absorption
        if distance_from_price > 40:
            tracking['status'] = 'done'
            continue
        
        # === BOUNDARY CONDITION 2: Price escaped (best bid/ask more aggressive than key level) ===
        # Means price just blasted through without absorbing orders
        price_escaped = False
        if side == 'support' and best_bid > price:
            # Best bid jumped above support = price escaped upward
            price_escaped = True
        elif side == 'resistance' and best_ask < price:
            # Best ask jumped below resistance = price escaped downward
            price_escaped = True
        
        if price_escaped:
            tracking['status'] = 'done'
            continue
        
        # === BOUNDARY CONDITION 3: Absorption confirmed ===
        # Orders at level dropped 60%+ AND price broke through
        orders_reduction = (peak_orders - orders_at_level) / peak_orders if peak_orders > 0 else 0
        
        price_broke_through = (
            (side == 'support' and current_price < price) or
            (side == 'resistance' and current_price > price)
        )
        
        if orders_reduction >= 0.60 and price_broke_through:
            absorptions.append({
                'price': price,
                'side': side,
                'orders_peak': peak_orders,
                'orders_now': orders_at_level,
                'reduction_pct': int(orders_reduction * 100),
                'time_to_absorption': time_elapsed,
                'started_at': tracking['started_at'].isoformat(),
                'snapshots_to_absorption': tracking['snapshot_count'],
                'status': 'absorbed',
                'price_broke': True,
                'current_price': current_price,
                'distance_at_absorption': distance_from_price
            })
            tracking['status'] = 'done'
            continue
        
        # === BOUNDARY CONDITION 4: Safety cleanup at 120 seconds ===
        # If none of above conditions triggered, stop monitoring
        if time_elapsed > 120:
            tracking['status'] = 'done'
            continue
    
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
