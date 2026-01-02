"""
Data Enricher
Transforms raw Kite ticks into enriched format for database storage
Adds instrument metadata and calculates derived metrics
"""

import redis
import psycopg2
import structlog
from typing import Dict, Optional, List
from datetime import datetime
from zoneinfo import ZoneInfo
from models import KiteTick, EnrichedTick, InstrumentInfo, DhanTick

logger = structlog.get_logger()

# IST timezone for timestamp conversion
IST = ZoneInfo('Asia/Kolkata')


def load_instruments_cache(database_url: str, redis_client: Optional[redis.Redis] = None) -> Dict[int, InstrumentInfo]:
    """
    Load instruments metadata from Postgres (with optional Redis fallback)
    
    Primary source: PostgreSQL
    Fallback: Redis (if provided and DB fails)
    
    Args:
        database_url: PostgreSQL connection URL
        redis_client: Optional Redis client for fallback
    
    Returns:
        Dict mapping instrument_token to InstrumentInfo
    """
    instruments_cache = {}
    
    # Try loading from Postgres first
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                instrument_token, trading_symbol, exchange, segment,
                instrument_type, name, expiry, strike, tick_size, lot_size,
                security_id, source
            FROM instruments
            WHERE is_active = TRUE
        """)
        
        rows = cursor.fetchall()
        
        logger.info("loading_instruments_from_db", total_instruments=len(rows))
        
        for row in rows:
            try:
                instrument = InstrumentInfo(
                    instrument_token=row[0],
                    trading_symbol=row[1],
                    exchange=row[2],
                    instrument_type=row[4],
                    expiry=row[6].isoformat() if row[6] else None,
                    strike=float(row[7]) if row[7] else None,
                    lot_size=int(row[9]) if row[9] else None,
                    tick_size=float(row[8]) if row[8] else None,
                    security_id=row[10],  # Dhan security_id
                    source=row[11] if row[11] else "kite"  # Data source
                )
                instruments_cache[row[0]] = instrument
            except Exception as e:
                logger.error("instrument_parse_failed", row=row, error=str(e))
                continue
        
        logger.info(
            "instruments_cache_loaded_from_db",
            total_instruments=len(instruments_cache)
        )
        
        cursor.close()
        conn.close()
        
        return instruments_cache
    
    except psycopg2.Error as db_error:
        logger.error("db_load_failed", error=str(db_error))
        
        # Cleanup connections before fallback
        if 'cursor' in locals():
            try:
                cursor.close()
            except:
                pass
        if 'conn' in locals():
            try:
                conn.close()
            except:
                pass
        
        # Fallback to Redis if database fails
        if redis_client:
            logger.warning("falling_back_to_redis_cache")
            try:
                pattern = "instrument:*"
                keys = redis_client.keys(pattern)
                
                logger.info("loading_from_redis_fallback", total_keys=len(keys))
                
                for key in keys:
                    try:
                        token_str = key.decode('utf-8').split(':')[1]
                        token = int(token_str)
                        data = redis_client.hgetall(key)
                        
                        if data:
                            decoded_data = {
                                k.decode('utf-8'): v.decode('utf-8') 
                                for k, v in data.items()
                            }
                            
                            instrument = InstrumentInfo(
                                instrument_token=token,
                                trading_symbol=decoded_data.get('tradingsymbol', ''),
                                exchange=decoded_data.get('exchange', ''),
                                instrument_type=decoded_data.get('instrument_type'),
                                expiry=decoded_data.get('expiry'),
                                strike=float(decoded_data['strike']) if decoded_data.get('strike') else None,
                                lot_size=int(decoded_data['lot_size']) if decoded_data.get('lot_size') else None,
                                tick_size=float(decoded_data['tick_size']) if decoded_data.get('tick_size') else None
                            )
                            instruments_cache[token] = instrument
                    except Exception as e:
                        logger.error("redis_instrument_load_failed", key=key, error=str(e))
                        continue
                
                logger.info("instruments_loaded_from_redis_fallback", count=len(instruments_cache))
                return instruments_cache
            
            except Exception as redis_error:
                logger.error("redis_fallback_failed", error=str(redis_error))
                return {}
        else:
            logger.error("no_fallback_available", message="Database failed and no Redis client provided")
            return {}
    
    # If we reach here without returning, DB succeeded
    return instruments_cache


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
    
    # Convert timestamps from UTC to IST
    utc_time = raw_tick.timestamp or datetime.utcnow()
    if utc_time.tzinfo is None:
        # If naive datetime, assume UTC
        utc_time = utc_time.replace(tzinfo=ZoneInfo('UTC'))
    ist_time = utc_time.astimezone(IST)
    
    # Convert last_trade_time if present
    ist_last_trade_time = None
    if raw_tick.last_trade_time:
        if raw_tick.last_trade_time.tzinfo is None:
            ist_last_trade_time = raw_tick.last_trade_time.replace(tzinfo=ZoneInfo('UTC')).astimezone(IST)
        else:
            ist_last_trade_time = raw_tick.last_trade_time.astimezone(IST)
    
    # Create enriched tick
    enriched = EnrichedTick(
        # Timestamps in IST
        time=ist_time,
        last_trade_time=ist_last_trade_time,
        
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


def dhan_tick_to_enriched(
    raw_tick: DhanTick,
    instruments_cache: Dict[int, InstrumentInfo]
) -> Optional[EnrichedTick]:
    """
    Transform Dhan tick into enriched format for database
    
    Maps Dhan's security_id → instrument_token using instruments cache
    Calculates change/change_percent from prev_close
    Converts Dhan market depth to standard format
    
    Args:
        raw_tick: Parsed Dhan tick from binary packet
        instruments_cache: Dict mapping instrument_token to InstrumentInfo
    
    Returns:
        EnrichedTick ready for database, or None if security_id not found
    """
    # Find instrument by security_id
    instrument_info = None
    instrument_token = None
    
    for token, info in instruments_cache.items():
        if info.security_id == raw_tick.security_id:
            instrument_info = info
            instrument_token = token
            break
    
    if not instrument_token:
        logger.warning(
            "security_id_not_found",
            security_id=raw_tick.security_id,
            exchange=raw_tick.exchange_segment
        )
        return None
    
    # Calculate change and change_percent from prev_close
    change = None
    change_percent = None
    
    if raw_tick.last_price and raw_tick.prev_close and raw_tick.prev_close > 0:
        change = round(raw_tick.last_price - raw_tick.prev_close, 2)
        change_percent = round((change / raw_tick.prev_close) * 100, 4)
    
    # Extract market depth arrays from Dhan's 5-level depth
    bid_prices: List[Optional[float]] = [None] * 5
    bid_quantities: List[Optional[int]] = [None] * 5
    bid_orders: List[Optional[int]] = [None] * 5
    
    ask_prices: List[Optional[float]] = [None] * 5
    ask_quantities: List[Optional[int]] = [None] * 5
    ask_orders: List[Optional[int]] = [None] * 5
    
    if raw_tick.depth:
        for i, level in enumerate(raw_tick.depth[:5]):
            bid_prices[i] = level.bid_price
            bid_quantities[i] = level.bid_quantity
            bid_orders[i] = level.bid_orders
            ask_prices[i] = level.ask_price
            ask_quantities[i] = level.ask_quantity
            ask_orders[i] = level.ask_orders
    
    # Calculate derived metrics
    bid_ask_spread = _calculate_spread(bid_prices[0], ask_prices[0])
    mid_price = _calculate_mid_price(bid_prices[0], ask_prices[0])
    order_imbalance = _calculate_order_imbalance(
        raw_tick.total_buy_quantity,
        raw_tick.total_sell_quantity
    )
    
    # Timestamp conversion (Dhan provides IST timestamps)
    ist_time = raw_tick.last_trade_time or datetime.now(IST)
    if ist_time.tzinfo is None:
        ist_time = ist_time.replace(tzinfo=IST)
    
    # Create enriched tick
    enriched = EnrichedTick(
        # Timestamps (already in IST from Dhan)
        time=ist_time,
        last_trade_time=raw_tick.last_trade_time,
        
        # Instrument identification
        instrument_token=instrument_token,
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
        day_open=raw_tick.day_open,
        day_high=raw_tick.day_high,
        day_low=raw_tick.day_low,
        day_close=raw_tick.day_close,
        
        # Change (calculated from prev_close)
        change=change,
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
        tradable=True,
        mode="full" if raw_tick.depth else "quote",
        
        # Derived fields
        bid_ask_spread=bid_ask_spread,
        mid_price=mid_price,
        order_imbalance=order_imbalance
    )
    
    return enriched


