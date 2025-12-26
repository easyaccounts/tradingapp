"""
Pydantic Models for KiteConnect WebSocket Data
Aligned with actual Kite API response structure
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field


class MarketDepthItem(BaseModel):
    """Single level in market depth (bid or ask)"""
    quantity: int = 0
    price: float = 0.0
    orders: int = 0


class MarketDepth(BaseModel):
    """Complete market depth with 5 levels each for buy and sell"""
    buy: List[MarketDepthItem] = Field(default_factory=lambda: [MarketDepthItem() for _ in range(5)])
    sell: List[MarketDepthItem] = Field(default_factory=lambda: [MarketDepthItem() for _ in range(5)])


class KiteTick(BaseModel):
    """
    KiteConnect WebSocket Tick Model
    Matches the actual structure from Kite API
    
    Reference: https://kite.trade/docs/connect/v3/websocket/#quote-and-full-mode
    """
    # Core identification
    tradable: bool = True
    mode: str = "quote"  # 'ltp', 'quote', or 'full'
    instrument_token: int
    
    # Price data
    last_price: Optional[float] = None
    last_traded_quantity: Optional[int] = None
    average_traded_price: Optional[float] = None
    
    # Volume data
    volume_traded: Optional[int] = None
    total_buy_quantity: Optional[int] = None
    total_sell_quantity: Optional[int] = None
    
    # OHLC (open, high, low, close)
    ohlc: Optional[Dict[str, float]] = None  # {'open': x, 'high': y, 'low': z, 'close': w}
    
    # Change
    change: Optional[float] = None
    
    # Timestamps
    last_trade_time: Optional[datetime] = None
    timestamp: Optional[datetime] = None
    
    # Open Interest (for derivatives)
    oi: Optional[int] = None
    oi_day_high: Optional[int] = None
    oi_day_low: Optional[int] = None
    
    # Market Depth (only in 'full' mode)
    depth: Optional[MarketDepth] = None
    
    class Config:
        arbitrary_types_allowed = True


class EnrichedTick(BaseModel):
    """
    Enriched tick model ready for database insertion
    Includes all transformations and derived metrics
    """
    # Timestamps
    time: datetime
    last_trade_time: Optional[datetime] = None
    
    # Instrument identification
    instrument_token: int
    trading_symbol: Optional[str] = None
    exchange: Optional[str] = None
    instrument_type: Optional[str] = None
    
    # Price data
    last_price: Optional[float] = None
    last_traded_quantity: Optional[int] = None
    average_traded_price: Optional[float] = None
    
    # Volume & OI
    volume_traded: Optional[int] = None
    oi: Optional[int] = None
    oi_day_high: Optional[int] = None
    oi_day_low: Optional[int] = None
    
    # OHLC (Day candle)
    day_open: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    day_close: Optional[float] = None
    
    # Change metrics
    change: Optional[float] = None
    change_percent: Optional[float] = None
    
    # Order book totals
    total_buy_quantity: Optional[int] = None
    total_sell_quantity: Optional[int] = None
    
    # Market Depth - Bids (5 levels)
    bid_prices: List[Optional[float]] = Field(default_factory=lambda: [None] * 5)
    bid_quantities: List[Optional[int]] = Field(default_factory=lambda: [None] * 5)
    bid_orders: List[Optional[int]] = Field(default_factory=lambda: [None] * 5)
    
    # Market Depth - Asks (5 levels)
    ask_prices: List[Optional[float]] = Field(default_factory=lambda: [None] * 5)
    ask_quantities: List[Optional[int]] = Field(default_factory=lambda: [None] * 5)
    ask_orders: List[Optional[int]] = Field(default_factory=lambda: [None] * 5)
    
    # Metadata
    tradable: bool = True
    mode: Optional[str] = None
    
    # Derived fields
    bid_ask_spread: Optional[float] = None
    mid_price: Optional[float] = None
    order_imbalance: Optional[int] = None
    
    class Config:
        arbitrary_types_allowed = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        data = self.model_dump()
        
        # Convert datetime objects to ISO format strings
        if self.time:
            data['time'] = self.time.isoformat()
        if self.last_trade_time:
            data['last_trade_time'] = self.last_trade_time.isoformat()
        
        return data


class InstrumentInfo(BaseModel):
    """Instrument metadata from instruments cache"""
    instrument_token: int
    trading_symbol: str
    exchange: str
    instrument_type: Optional[str] = None
    expiry: Optional[str] = None
    strike: Optional[float] = None
    lot_size: Optional[int] = None
    tick_size: Optional[float] = None
