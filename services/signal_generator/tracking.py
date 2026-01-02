"""
Level Tracking Module
Tracks order book levels from discovery to resolution
"""

from datetime import datetime
from typing import Optional


class TrackedLevel:
    """Represents a tracked price level with order concentration"""
    
    def __init__(self, price: float, side: str, initial_orders: int, initial_quantity: int, timestamp: datetime):
        self.price = float(price)  # Ensure float type
        self.side = side  # 'support' or 'resistance'
        self.first_seen = timestamp
        self.peak_orders = initial_orders
        self.current_orders = initial_orders
        self.peak_quantity = initial_quantity
        self.current_quantity = initial_quantity
        self.order_history = [(timestamp, initial_orders)]
        self.quantity_history = [(timestamp, initial_quantity)]
        self.price_distance_history = []
        self.price_touched = False
        self.tests = 0  # How many times price tested this level
        self.status = 'forming'  # forming → active → breaking → broken
        self.last_updated = timestamp
        self.active = True  # Whether level is currently in depth snapshot
        self.last_seen = timestamp  # Last time observed in depth
        self.inactive_since = None  # When level became inactive (None if active)
    
    def update(self, orders: int, quantity: int, current_price: float, timestamp: datetime):
        """Update level with new data"""
        current_price = float(current_price)  # Ensure float type
        self.current_orders = orders
        self.current_quantity = quantity
        self.last_updated = timestamp
        
        # Mark as active and update last_seen
        self.active = True
        self.last_seen = timestamp
        self.inactive_since = None
        
        # Track history (keep last 60 snapshots)
        self.order_history.append((timestamp, orders))
        if len(self.order_history) > 60:
            self.order_history.pop(0)
        self.quantity_history.append((timestamp, quantity))
        if len(self.quantity_history) > 60:
            self.quantity_history.pop(0)
        
        # Track price distance
        distance = abs(current_price - self.price)
        self.price_distance_history.append((timestamp, distance))
        if len(self.price_distance_history) > 60:
            self.price_distance_history.pop(0)
        
        # Check if price touched this level
        if distance <= 5:  # Within 5 points
            self.price_touched = True
            self.tests += 1
        
        # Update peaks
        if orders > self.peak_orders:
            self.peak_orders = orders
        if quantity > self.peak_quantity:
            self.peak_quantity = quantity
        
        # Update status
        if self.age_seconds > 10:
            self.status = 'active'
    
    @property
    def age_seconds(self) -> int:
        """Age of this level in seconds"""
        return int((self.last_updated - self.first_seen).total_seconds())
    
    @property
    def age_display(self) -> str:
        """Human-readable age"""
        age = self.age_seconds
        if age < 60:
            return f"{age}s"
        elif age < 3600:
            return f"{age // 60}m {age % 60}s"
        else:
            return f"{age // 3600}h {(age % 3600) // 60}m"

    @property
    def avg_quantity(self) -> float:
        """Average resting quantity over stored history"""
        if not self.quantity_history:
            return 0.0
        return sum(q for _, q in self.quantity_history) / len(self.quantity_history)
    
    def get_order_trend(self, window: int = 20) -> str:
        """Analyze if orders are increasing, decreasing, or stable"""
        if len(self.order_history) < window:
            return 'unknown'
        
        recent = [orders for _, orders in self.order_history[-window:]]
        
        # Count increasing vs decreasing pairs
        increases = sum(1 for i in range(len(recent)-1) if recent[i+1] > recent[i])
        decreases = sum(1 for i in range(len(recent)-1) if recent[i+1] < recent[i])
        
        if increases > decreases * 1.5:
            return 'increasing'
        elif decreases > increases * 1.5:
            return 'decreasing'
        else:
            return 'stable'
    
    def is_consistent_decline(self, window: int = 20) -> bool:
        """Check if orders are consistently declining (not flickering)"""
        if len(self.order_history) < window:
            return False
        
        recent = [orders for _, orders in self.order_history[-window:]]
        
        # At least 70% of comparisons should be declines
        declines = sum(1 for i in range(len(recent)-1) if recent[i] > recent[i+1])
        return declines / (len(recent) - 1) > 0.7
    
    def mark_inactive(self, timestamp: datetime):
        """Mark level as inactive (not currently in depth)"""
        if self.active:
            self.active = False
            self.inactive_since = timestamp
    
    @property
    def inactive_duration(self) -> int:
        """Seconds since level became inactive (0 if active)"""
        if self.active or self.inactive_since is None:
            return 0
        return int((self.last_updated - self.inactive_since).total_seconds())
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'price': float(self.price),
            'side': self.side,
            'first_seen': self.first_seen.strftime('%H:%M:%S') if self.first_seen else 'N/A',
            'orders': self.current_orders,
            'peak_orders': self.peak_orders,
            'quantity': self.current_quantity,
            'peak_quantity': self.peak_quantity,
            'age_seconds': self.age_seconds,
            'age_display': self.age_display,
            'tests': self.tests,
            'status': self.status,
            'price_touched': self.price_touched,
            'trend': self.get_order_trend(),
            'active': self.active,
            'inactive_duration': self.inactive_duration
        }


class LevelTracker:
    """Manages multiple tracked levels"""
    
    def __init__(self):
        self.levels = {}  # price -> TrackedLevel
    
    def add_level(self, price: float, side: str, orders: int, quantity: int, timestamp: datetime):
        """Add a new level to track"""
        price = float(price)  # Ensure float type
        self.levels[price] = TrackedLevel(price, side, orders, quantity, timestamp)
    
    def update_level(self, price: float, orders: int, quantity: int, current_price: float, timestamp: datetime):
        """Update existing level"""
        price = float(price)  # Ensure float type
        current_price = float(current_price)  # Ensure float type
        if price in self.levels:
            self.levels[price].update(orders, quantity, current_price, timestamp)
    
    def remove_level(self, price: float):
        """Remove a level from tracking"""
        price = float(price)  # Ensure float type
        if price in self.levels:
            del self.levels[price]
    
    def get_level(self, price: float) -> Optional[TrackedLevel]:
        """Get a specific level"""
        price = float(price)  # Ensure float type
        return self.levels.get(price)
    
    def get_all_levels(self) -> list:
        """Get all tracked levels"""
        return list(self.levels.values())
    
    def mark_absent_levels_inactive(self, current_big_levels: dict, timestamp: datetime):
        """Mark levels not in current snapshot as inactive"""
        current_prices = set(current_big_levels.keys())
        
        for price, level in self.levels.items():
            if price not in current_prices:
                level.mark_inactive(timestamp)
    
    def cleanup_stale_levels(self, current_price: float, timestamp: datetime, max_age: int = 600, max_distance: int = 150, max_inactive: int = 600):
        """Remove levels that are no longer relevant"""
        to_remove = []
        
        for price, level in self.levels.items():
            # Remove if too far from price
            if abs(price - current_price) > max_distance:
                to_remove.append(price)
                continue
            
            # Remove if inactive for too long (10 minutes)
            if not level.active and level.inactive_since:
                inactive_duration = (timestamp - level.inactive_since).total_seconds()
                if inactive_duration > max_inactive:
                    to_remove.append(price)
                    continue
            
            # Remove if too old and never tested
            if level.age_seconds > max_age and not level.price_touched:
                to_remove.append(price)
                continue
            
            # Remove if orders dropped to insignificance and not tested (only for active levels)
            if level.active and level.current_orders < 20 and not level.price_touched:
                to_remove.append(price)
                continue
        
        for price in to_remove:
            self.remove_level(price)
        
        return len(to_remove)
