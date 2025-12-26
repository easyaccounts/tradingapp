from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timedelta
import asyncpg
from redis import Redis
import os
from typing import List, Dict, Tuple
from enum import Enum

router = APIRouter()

class TimeInterval(str, Enum):
    """Time interval options for historical analysis"""
    FIFTEEN_MIN = "15m"
    THIRTY_MIN = "30m"
    ONE_HOUR = "1h"
    ALL_DAY = "all"

# Redis connection
def get_redis_client() -> Redis:
    """Get Redis client"""
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return Redis.from_url(redis_url, decode_responses=True)  # type: ignore

# Database connection
async def get_db_connection():
    """Create database connection from DATABASE_URL"""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")
    
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    
    try:
        return await asyncpg.connect(database_url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")


def get_nifty_instruments_by_expiry(redis_client: Redis) -> Tuple[List[int], List[int], List[int]]:
    """
    Get NIFTY instrument tokens filtered by 2 nearest expiries
    
    Returns:
        Tuple of (futures_tokens, calls_tokens, puts_tokens)
    """
    expiry_map = {}
    futures_list = []
    calls_list = []
    puts_list = []
    
    # Scan all instruments in Redis
    for key in redis_client.scan_iter("instrument:*"):
        token = key.split(":")[-1]
        instrument_data: dict = redis_client.hgetall(key)  # type: ignore
        
        segment = instrument_data.get('segment', '')
        name = instrument_data.get('name', '')
        tradingsymbol = instrument_data.get('tradingsymbol', '')
        expiry_str = instrument_data.get('expiry', '')
        
        # Filter for NIFTY only
        if name != 'NIFTY' or not expiry_str or expiry_str == 'None':
            continue
        
        try:
            expiry_date = datetime.strptime(expiry_str.split()[0], '%Y-%m-%d').date()
            token_int = int(token)
            expiry_map[token_int] = expiry_date
            
            # Categorize by segment and symbol
            if segment == 'NFO-FUT':
                futures_list.append((token_int, expiry_date))
            elif segment == 'NFO-OPT':
                if tradingsymbol.endswith('CE'):
                    calls_list.append((token_int, expiry_date))
                elif tradingsymbol.endswith('PE'):
                    puts_list.append((token_int, expiry_date))
        except:
            continue
    
    # Get 2 nearest expiries
    unique_expiries = sorted(set(expiry_map.values()))[:2]
    
    # Filter to only 2 nearest expiries
    futures_tokens = [token for token, expiry in futures_list if expiry in unique_expiries]
    calls_tokens = [token for token, expiry in calls_list if expiry in unique_expiries]
    puts_tokens = [token for token, expiry in puts_list if expiry in unique_expiries]
    
    return futures_tokens, calls_tokens, puts_tokens


async def calculate_cvd(conn, tokens: List[int], start_time: datetime) -> float:
    """
    Calculate aggregated CVD for given tokens
    
    Args:
        conn: Database connection
        tokens: List of instrument tokens
        start_time: Start time for CVD calculation
    
    Returns:
        Aggregated CVD value
    """
    if not tokens:
        return 0.0
    
    data = await conn.fetch("""
        SELECT 
            time,
            instrument_token,
            volume_traded,
            last_price,
            bid_prices,
            ask_prices
        FROM ticks
        WHERE instrument_token = ANY($1) 
            AND time >= $2
        ORDER BY instrument_token, time ASC
    """, tokens, start_time)
    
    cvd_total = 0.0
    prev_volumes = {}  # Track previous volume per token
    
    for row in data:
        token = row['instrument_token']
        current_volume = float(row['volume_traded'] or 0)
        last_price = float(row['last_price'] or 0)
        bid_prices = row['bid_prices'] or []
        ask_prices = row['ask_prices'] or []
        
        prev_volume = prev_volumes.get(token)
        
        if prev_volume is not None and current_volume > prev_volume and last_price > 0:
            volume_delta = float(current_volume - prev_volume)
            
            best_bid = float(bid_prices[0]) if bid_prices and len(bid_prices) > 0 and bid_prices[0] else 0
            best_ask = float(ask_prices[0]) if ask_prices and len(ask_prices) > 0 and ask_prices[0] else 0
            
            # Determine aggressor side
            if best_ask > 0 and last_price >= best_ask:
                cvd_change = volume_delta  # Buyer aggressor
            elif best_bid > 0 and last_price <= best_bid:
                cvd_change = -volume_delta  # Seller aggressor
            else:
                # Use mid-price comparison
                if best_bid > 0 and best_ask > 0:
                    mid_price = (best_bid + best_ask) / 2
                    cvd_change = volume_delta if last_price > mid_price else -volume_delta
                else:
                    cvd_change = 0
            
            cvd_total += cvd_change
        
        if current_volume > 0:
            prev_volumes[token] = current_volume
    
    return cvd_total


async def calculate_volume_and_oi(conn, tokens: List[int], start_time: datetime) -> Tuple[float, float]:
    """
    Calculate aggregated volume and OI change
    
    Returns:
        Tuple of (total_volume, oi_change)
    """
    if not tokens:
        return 0.0, 0.0
    
    data = await conn.fetchrow("""
        SELECT 
            SUM(CASE 
                WHEN latest.volume_traded IS NOT NULL AND earliest.volume_traded IS NOT NULL 
                THEN latest.volume_traded - earliest.volume_traded 
                ELSE 0 
            END) as total_volume,
            SUM(CASE 
                WHEN latest.oi IS NOT NULL AND earliest.oi IS NOT NULL 
                THEN latest.oi - earliest.oi 
                ELSE 0 
            END) as oi_change
        FROM (
            SELECT DISTINCT ON (instrument_token)
                instrument_token,
                volume_traded,
                oi
            FROM ticks
            WHERE instrument_token = ANY($1) AND time >= $2
            ORDER BY instrument_token, time DESC
        ) latest
        LEFT JOIN (
            SELECT DISTINCT ON (instrument_token)
                instrument_token,
                volume_traded as earliest_volume,
                oi as earliest_oi
            FROM ticks
            WHERE instrument_token = ANY($1) AND time >= $2
            ORDER BY instrument_token, time ASC
        ) earliest ON latest.instrument_token = earliest.instrument_token
    """, tokens, start_time)
    
    total_volume = float(data['total_volume'] or 0)
    oi_change = float(data['oi_change'] or 0)
    
    return total_volume, oi_change


async def calculate_order_book_imbalance(conn, tokens: List[int]) -> Tuple[float, float]:
    """
    Calculate value-weighted order book imbalance
    
    Returns:
        Tuple of (total_bid_value, total_ask_value)
    """
    if not tokens:
        return 0.0, 0.0
    
    # Get latest ticks for all tokens
    data = await conn.fetch("""
        SELECT DISTINCT ON (instrument_token)
            instrument_token,
            last_price,
            total_buy_quantity,
            total_sell_quantity
        FROM ticks
        WHERE instrument_token = ANY($1)
        ORDER BY instrument_token, time DESC
    """, tokens)
    
    total_bid_value = 0.0
    total_ask_value = 0.0
    
    for row in data:
        price = float(row['last_price'] or 0)
        buy_qty = float(row['total_buy_quantity'] or 0)
        sell_qty = float(row['total_sell_quantity'] or 0)
        
        if price > 0:
            total_bid_value += buy_qty * price
            total_ask_value += sell_qty * price
    
    return total_bid_value, total_ask_value


@router.get("/orderflow")
async def get_orderflow_analysis():
    """
    Get aggregated orderflow analysis for all NIFTY instruments (2 nearest expiries)
    
    Returns:
    - market_overview: NIFTY futures price, volume, OI changes
    - cvd: CVD broken by futures/calls/puts
    - volume: Volume broken by futures/calls/puts
    - oi_changes: OI changes broken by futures/calls/puts
    - imbalance: Order book value-weighted imbalance by futures/calls/puts
    """
    
    conn = await get_db_connection()
    redis_client = get_redis_client()
    
    try:
        # Get NIFTY instrument tokens (2 nearest expiries)
        futures_tokens, calls_tokens, puts_tokens = get_nifty_instruments_by_expiry(redis_client)
        redis_client.close()
        
        if not futures_tokens:
            raise HTTPException(status_code=404, detail="No NIFTY futures found")
        
        # Time windows
        now = datetime.utcnow()
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Get latest NIFTY futures price for market overview
        latest_future = await conn.fetchrow("""
            SELECT 
                last_price,
                volume_traded,
                oi
            FROM ticks
            WHERE instrument_token = ANY($1)
            ORDER BY time DESC
            LIMIT 1
        """, futures_tokens)
        
        current_price = float(latest_future['last_price'] or 0) if latest_future else 0.0
        
        # Get day open price for futures
        day_open = await conn.fetchval("""
            SELECT last_price
            FROM ticks
            WHERE instrument_token = ANY($1)
                AND time >= $2
            ORDER BY time ASC
            LIMIT 1
        """, futures_tokens, day_start)
        
        day_open = float(day_open or current_price)
        price_change = current_price - day_open
        price_change_pct = (price_change / day_open * 100) if day_open > 0 else 0.0
        
        # Calculate CVD for each category
        futures_cvd = await calculate_cvd(conn, futures_tokens, day_start)
        calls_cvd = await calculate_cvd(conn, calls_tokens, day_start)
        puts_cvd = await calculate_cvd(conn, puts_tokens, day_start)
        
        # Calculate volume and OI changes
        futures_volume, futures_oi_change = await calculate_volume_and_oi(conn, futures_tokens, day_start)
        calls_volume, calls_oi_change = await calculate_volume_and_oi(conn, calls_tokens, day_start)
        puts_volume, puts_oi_change = await calculate_volume_and_oi(conn, puts_tokens, day_start)
        
        # Calculate order book imbalance (value-weighted)
        futures_bid_value, futures_ask_value = await calculate_order_book_imbalance(conn, futures_tokens)
        calls_bid_value, calls_ask_value = await calculate_order_book_imbalance(conn, calls_tokens)
        puts_bid_value, puts_ask_value = await calculate_order_book_imbalance(conn, puts_tokens)
        
        # Construct response
        response = {
            "market_overview": {
                "symbol": "NIFTY",
                "price": round(float(current_price), 2),
                "change": round(float(price_change), 2),
                "change_pct": round(float(price_change_pct), 2)
            },
            "cvd": {
                "futures": round(float(futures_cvd), 2),
                "calls": round(float(calls_cvd), 2),
                "puts": round(float(puts_cvd), 2),
                "net_options": round(float(calls_cvd - puts_cvd), 2)
            },
            "volume": {
                "futures": round(float(futures_volume), 2),
                "calls": round(float(calls_volume), 2),
                "puts": round(float(puts_volume), 2),
                "total": round(float(futures_volume + calls_volume + puts_volume), 2)
            },
            "oi_changes": {
                "futures": round(float(futures_oi_change), 2),
                "calls": round(float(calls_oi_change), 2),
                "puts": round(float(puts_oi_change), 2),
                "total": round(float(futures_oi_change + calls_oi_change + puts_oi_change), 2)
            },
            "imbalance": {
                "futures": {
                    "bid_value": round(float(futures_bid_value), 2),
                    "ask_value": round(float(futures_ask_value), 2),
                    "ratio": round(float(futures_bid_value / futures_ask_value if futures_ask_value > 0 else 0), 2)
                },
                "calls": {
                    "bid_value": round(float(calls_bid_value), 2),
                    "ask_value": round(float(calls_ask_value), 2),
                    "ratio": round(float(calls_bid_value / calls_ask_value if calls_ask_value > 0 else 0), 2)
                },
                "puts": {
                    "bid_value": round(float(puts_bid_value), 2),
                    "ask_value": round(float(puts_ask_value), 2),
                    "ratio": round(float(puts_bid_value / puts_ask_value if puts_ask_value > 0 else 0), 2)
                }
            },
            "metadata": {
                "futures_count": len(futures_tokens),
                "calls_count": len(calls_tokens),
                "puts_count": len(puts_tokens),
                "timestamp": now.isoformat()
            }
        }
        
        return response
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")
    
    finally:
        await conn.close()


@router.get("/orderflow/history")
async def get_historical_analysis(
    interval: TimeInterval = Query(TimeInterval.ALL_DAY, description="Time interval for analysis")
):
    """
    Get historical aggregated orderflow analysis with time intervals
    
    Args:
        interval: Time window (15m, 30m, 1h, all)
    
    Returns:
    - Same structure as /orderflow but for specified time window
    - price_timeline: 5-min buckets with price/volume/oi
    """
    
    conn = await get_db_connection()
    redis_client = get_redis_client()
    
    try:
        # Get NIFTY instrument tokens
        futures_tokens, calls_tokens, puts_tokens = get_nifty_instruments_by_expiry(redis_client)
        redis_client.close()
        
        if not futures_tokens:
            raise HTTPException(status_code=404, detail="No NIFTY futures found")
        
        # Determine time window
        now = datetime.utcnow()
        if interval == TimeInterval.FIFTEEN_MIN:
            start_time = now - timedelta(minutes=15)
        elif interval == TimeInterval.THIRTY_MIN:
            start_time = now - timedelta(minutes=30)
        elif interval == TimeInterval.ONE_HOUR:
            start_time = now - timedelta(hours=1)
        else:  # ALL_DAY
            start_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Calculate metrics for time window
        futures_cvd = await calculate_cvd(conn, futures_tokens, start_time)
        calls_cvd = await calculate_cvd(conn, calls_tokens, start_time)
        puts_cvd = await calculate_cvd(conn, puts_tokens, start_time)
        
        futures_volume, futures_oi_change = await calculate_volume_and_oi(conn, futures_tokens, start_time)
        calls_volume, calls_oi_change = await calculate_volume_and_oi(conn, calls_tokens, start_time)
        puts_volume, puts_oi_change = await calculate_volume_and_oi(conn, puts_tokens, start_time)
        
        # Get 5-minute timeline data for futures
        timeline = await conn.fetch("""
            SELECT 
                time_bucket('5 minutes', time) as bucket,
                AVG(last_price) as avg_price,
                SUM(CASE 
                    WHEN latest.volume_traded > earliest.volume_traded 
                    THEN latest.volume_traded - earliest.volume_traded 
                    ELSE 0 
                END) as volume,
                AVG(oi) as avg_oi
            FROM ticks
            WHERE instrument_token = ANY($1) AND time >= $2
            GROUP BY bucket
            ORDER BY bucket ASC
        """, futures_tokens, start_time)
        
        price_timeline = []
        for row in timeline:
            price_timeline.append({
                "time": row['bucket'].strftime('%H:%M'),
                "price": round(float(row['avg_price'] or 0), 2),
                "volume": round(float(row['volume'] or 0), 2),
                "oi": round(float(row['avg_oi'] or 0), 2)
            })
        
        response = {
            "interval": interval.value,
            "start_time": start_time.isoformat(),
            "cvd": {
                "futures": round(float(futures_cvd), 2),
                "calls": round(float(calls_cvd), 2),
                "puts": round(float(puts_cvd), 2),
                "net_options": round(float(calls_cvd - puts_cvd), 2)
            },
            "volume": {
                "futures": round(float(futures_volume), 2),
                "calls": round(float(calls_volume), 2),
                "puts": round(float(puts_volume), 2),
                "total": round(float(futures_volume + calls_volume + puts_volume), 2)
            },
            "oi_changes": {
                "futures": round(float(futures_oi_change), 2),
                "calls": round(float(calls_oi_change), 2),
                "puts": round(float(puts_oi_change), 2),
                "total": round(float(futures_oi_change + calls_oi_change + puts_oi_change), 2)
            },
            "price_timeline": price_timeline
        }
        
        return response
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Historical analysis failed: {str(e)}")
    
    finally:
        await conn.close()
