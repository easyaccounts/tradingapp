"""
Data Enricher
Transforms raw Kite ticks into enriched format for database storage
Adds instrument metadata and calculates derived metrics
"""

import redis
import structlog
from typing import Dict, Optional, List
from datetime import datetime
from models import KiteTick, EnrichedTick, InstrumentInfo

logger = structlog.get_logger()


def load_instruments_cache(redis_client: redis.Redis) -> Dict[int, InstrumentInfo]:
    """
    Load instruments metadata from Redis into memory cache
    
    Args:
        redis_client: Redis client instance
    
    Returns:
        Dict mapping instrument_token to InstrumentInfo
    """
    instruments_cache = {}
    
    try:
        # Get all instrument keys from Redis
        # Expected format: instrument:{token} -> hash with fields
        pattern = "instrument:*"
        keys = redis_client.keys(pattern)
        
        logger.info("loading_instruments_cache", total_keys=len(keys))
        
        for key in keys:
            try:
                # Extract token from key
                token_str = key.decode('utf-8').split(':')[1]
                token = int(token_str)
                
                # Get instrument data
                data = redis_client.hgetall(key)
                
                if data:
                    # Decode bytes to strings
                    decoded_data = {
                        k.decode('utf-8'): v.decode('utf-8') 
                        for k, v in data.items()
                    }
                    
                    # Create InstrumentInfo with safe parsing
                    try:
                        strike = float(decoded_data['strike']) if decoded_data.get('strike') else None
                    except (ValueError, KeyError):
                        strike = None
                    
                    try:
                        lot_size = int(decoded_data['lot_size']) if decoded_data.get('lot_size') else None
                    except (ValueError, KeyError):
                        lot_size = None
                    
                    try:
                        tick_size = float(decoded_data['tick_size']) if decoded_data.get('tick_size') else None
                    except (ValueError, KeyError):
                        tick_size = None
                    
                    instrument = InstrumentInfo(
                        instrument_token=token,
                        trading_symbol=decoded_data.get('tradingsymbol', ''),
                        exchange=decoded_data.get('exchange', ''),
                        instrument_type=decoded_data.get('instrument_type'),
                        expiry=decoded_data.get('expiry'),
                        strike=strike,
                        lot_size=lot_size,
                        tick_size=tick_size
                    )
                    
                    instruments_cache[token] = instrument
            
            except Exception as e:
                logger.error("instrument_load_failed", key=key, error=str(e))
                continue
        
        logger.info(
            "instruments_cache_loaded",
            total_instruments=len(instruments_cache)
        )
        
        return instruments_cache
    
    except Exception as e:
        logger.error("cache_load_failed", error=str(e))
        return {}


def enrich_tick(
    raw_tick: KiteTick,
    instruments_cache: Dict[int, InstrumentInfo]
) -> EnrichedTick:
    """
    Transform raw Kite tick into enriched format for database
    
    Enrichment includes:
    - Adding instrument metadata (symbol, exchange, type)
    - Extracting market depth arrays
    - Calculating derived metrics (spread, mid-price, imbalance)
    - Structuring data to match database schema
    
    Args:
        raw_tick: Raw tick from Kite WebSocket
        instruments_cache: Dictionary of instrument metadata
    
    Returns:
        EnrichedTick: Enriched tick ready for database insertion
    """
    # Get instrument metadata
    instrument_info = instruments_cache.get(raw_tick.instrument_token)
    
    # Extract OHLC values
    day_open = None
    day_high = None
    day_low = None
    day_close = None
    
    if raw_tick.ohlc:
        day_open = raw_tick.ohlc.get('open')
        day_high = raw_tick.ohlc.get('high')
        day_low = raw_tick.ohlc.get('low')
        day_close = raw_tick.ohlc.get('close')
    
    # Extract market depth arrays
    bid_prices: List[Optional[float]] = [None] * 5
    bid_quantities: List[Optional[int]] = [None] * 5
    bid_orders: List[Optional[int]] = [None] * 5
    
    ask_prices: List[Optional[float]] = [None] * 5
    ask_quantities: List[Optional[int]] = [None] * 5
    ask_orders: List[Optional[int]] = [None] * 5
    
    if raw_tick.depth:
        # Extract bid side
        if raw_tick.depth.buy:
            for i, bid in enumerate(raw_tick.depth.buy[:5]):
                bid_prices[i] = bid.price if bid.price else None
                bid_quantities[i] = bid.quantity if bid.quantity else None
                bid_orders[i] = bid.orders if bid.orders else None
        
        # Extract ask side
        if raw_tick.depth.sell:
            for i, ask in enumerate(raw_tick.depth.sell[:5]):
                ask_prices[i] = ask.price if ask.price else None
                ask_quantities[i] = ask.quantity if ask.quantity else None
                ask_orders[i] = ask.orders if ask.orders else None
    
    # Calculate derived metrics
    bid_ask_spread = _calculate_spread(bid_prices[0], ask_prices[0])
    mid_price = _calculate_mid_price(bid_prices[0], ask_prices[0])
    order_imbalance = _calculate_order_imbalance(
        raw_tick.total_buy_quantity,
        raw_tick.total_sell_quantity
    )
    change_percent = _calculate_change_percent(raw_tick.change, day_close)
    
    # Create enriched tick
    enriched = EnrichedTick(
        # Timestamps (use UTC to avoid timezone issues)
        time=raw_tick.timestamp or datetime.utcnow(),
        last_trade_time=raw_tick.last_trade_time,
        
        # Instrument identification
        instrument_token=raw_tick.instrument_token,
        trading_symbol=instrument_info.trading_symbol if instrument_info else None,
        exchange=instrument_info.exchange if instrument_info else None,
        instrument_type=instrument_info.instrument_type if instrument_info else None,
        
        # Price data
        last_price=raw_tick.last_price,
        last_traded_quantity=raw_tick.last_traded_quantity,
        average_traded_price=raw_tick.average_traded_price,
        
        # Volume & OI
        volume_traded=raw_tick.volume_traded,
        oi=raw_tick.oi,
        oi_day_high=raw_tick.oi_day_high,
        oi_day_low=raw_tick.oi_day_low,
        
        # OHLC
        day_open=day_open,
        day_high=day_high,
        day_low=day_low,
        day_close=day_close,
        
        # Change
        change=raw_tick.change,
        change_percent=change_percent,
        
        # Order book totals
        total_buy_quantity=raw_tick.total_buy_quantity,
        total_sell_quantity=raw_tick.total_sell_quantity,
        
        # Market depth
        bid_prices=bid_prices,
        bid_quantities=bid_quantities,
        bid_orders=bid_orders,
        ask_prices=ask_prices,
        ask_quantities=ask_quantities,
        ask_orders=ask_orders,
        
        # Metadata
        tradable=raw_tick.tradable,
        mode=raw_tick.mode,
        
        # Derived fields
        bid_ask_spread=bid_ask_spread,
        mid_price=mid_price,
        order_imbalance=order_imbalance
    )
    
    return enriched


def _calculate_spread(best_bid: Optional[float], best_ask: Optional[float]) -> Optional[float]:
    """
    Calculate bid-ask spread
    
    Args:
        best_bid: Best bid price (highest)
        best_ask: Best ask price (lowest)
    
    Returns:
        Spread in absolute terms, or None if data unavailable
    """
    if best_bid is not None and best_ask is not None and best_bid > 0 and best_ask > 0:
        return round(best_ask - best_bid, 2)
    return None


def _calculate_mid_price(best_bid: Optional[float], best_ask: Optional[float]) -> Optional[float]:
    """
    Calculate mid-price (average of best bid and ask)
    
    Args:
        best_bid: Best bid price
        best_ask: Best ask price
    
    Returns:
        Mid-price, or None if data unavailable
    """
    if best_bid is not None and best_ask is not None and best_bid > 0 and best_ask > 0:
        return round((best_bid + best_ask) / 2, 2)
    return None


def _calculate_order_imbalance(
    total_buy: Optional[int],
    total_sell: Optional[int]
) -> Optional[int]:
    """
    Calculate order imbalance (buy quantity - sell quantity)
    Positive value indicates buying pressure, negative indicates selling pressure
    
    Args:
        total_buy: Total buy quantity in order book
        total_sell: Total sell quantity in order book
    
    Returns:
        Order imbalance, or None if data unavailable
    """
    if total_buy is not None and total_sell is not None:
        return total_buy - total_sell
    return None


def _calculate_change_percent(change: Optional[float], close: Optional[float]) -> Optional[float]:
    """
    Calculate percentage change from previous close
    
    Args:
        change: Absolute change in price
        close: Previous close price
    
    Returns:
        Percentage change, or None if data unavailable
    """
    if change is not None and close is not None and close > 0:
        return round((change / close) * 100, 4)
    return None
