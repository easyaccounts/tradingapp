from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timedelta
import asyncpg
from redis import Redis
import os
import asyncio
import time as time_module
from typing import List, Dict, Tuple, Optional
from enum import Enum

router = APIRouter()

class TimeInterval(str, Enum):
    """Time interval options for historical analysis"""
    FIFTEEN_MIN = "15m"
    THIRTY_MIN = "30m"
    ONE_HOUR = "1h"
    ALL_DAY = "all"

# Cache for instrument tokens (5 minute TTL)
_instruments_cache: Optional[Tuple[List[int], List[int], List[int]]] = None
_cache_timestamp: float = 0
CACHE_TTL = 300  # 5 minutes

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
    Uses caching to avoid repeated Redis scans
    
    Returns:
        Tuple of (futures_tokens, calls_tokens, puts_tokens)
    """
    global _instruments_cache, _cache_timestamp
    
    # Check cache
    current_time = time_module.time()
    if _instruments_cache and (current_time - _cache_timestamp) < CACHE_TTL:
        return _instruments_cache
    
    # Rebuild cache
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
    
    # Update cache
    _instruments_cache = (futures_tokens, calls_tokens, puts_tokens)
    _cache_timestamp = current_time
    
    return futures_tokens, calls_tokens, puts_tokens


async def calculate_cvd(conn, tokens: List[int], start_time: datetime) -> float:
    """
    Calculate aggregated CVD using pre-calculated cvd_change column
    Much faster than window functions (simple SUM aggregation)
    
    Args:
        conn: Database connection
        tokens: List of instrument tokens
        start_time: Start time for CVD calculation
    
    Returns:
        Aggregated CVD value
    """
    if not tokens:
        return 0.0
    
    # Simple SUM query on pre-calculated cvd_change
    result = await conn.fetchval("""
        SELECT COALESCE(SUM(cvd_change), 0) as total_cvd
        FROM ticks
        WHERE instrument_token = ANY($1) 
            AND time >= $2
            AND cvd_change IS NOT NULL
    """, tokens, start_time)
    
    return float(result or 0.0)


async def calculate_volume_and_oi(conn, tokens: List[int], start_time: datetime) -> Tuple[float, float]:
    """
    Calculate aggregated volume and OI change using pre-calculated deltas
    
    Returns:
        Tuple of (total_volume, oi_change)
    """
    if not tokens:
        return 0.0, 0.0
    
    result = await conn.fetchrow("""
        SELECT 
            COALESCE(SUM(volume_delta), 0) as total_volume,
            COALESCE(SUM(oi_delta), 0) as oi_change
        FROM ticks
        WHERE instrument_token = ANY($1) AND time >= $2
    """, tokens, start_time)
    
    if not result:
        return 0.0, 0.0
    
    total_volume = float(result['total_volume'] or 0)
    oi_change = float(result['oi_change'] or 0)
    
    return total_volume, oi_change


async def calculate_order_book_imbalance(conn, tokens: List[int]) -> Tuple[float, float]:
    """
    Calculate value-weighted order book imbalance using pre-calculated depth totals
    
    Returns:
        Tuple of (total_bid_value, total_ask_value)
    """
    if not tokens:
        return 0.0, 0.0
    
    # Get latest ticks for all tokens with pre-calculated depth
    data = await conn.fetch("""
        SELECT DISTINCT ON (instrument_token)
            instrument_token,
            last_price,
            bid_depth_total,
            ask_depth_total
        FROM ticks
        WHERE instrument_token = ANY($1)
            AND bid_depth_total IS NOT NULL
            AND ask_depth_total IS NOT NULL
        ORDER BY instrument_token, time DESC
    """, tokens)
    
    total_bid_value = 0.0
    total_ask_value = 0.0
    
    for row in data:
        price = float(row['last_price'] or 0)
        bid_depth = float(row['bid_depth_total'] or 0)
        ask_depth = float(row['ask_depth_total'] or 0)
        
        if price > 0:
            total_bid_value += bid_depth * price
            total_ask_value += ask_depth * price
    
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
        
        # Parallelize all calculations using asyncio.gather
        results = await asyncio.gather(
            calculate_cvd(conn, futures_tokens, day_start),
            calculate_cvd(conn, calls_tokens, day_start),
            calculate_cvd(conn, puts_tokens, day_start),
            calculate_volume_and_oi(conn, futures_tokens, day_start),
            calculate_volume_and_oi(conn, calls_tokens, day_start),
            calculate_volume_and_oi(conn, puts_tokens, day_start),
            calculate_order_book_imbalance(conn, futures_tokens),
            calculate_order_book_imbalance(conn, calls_tokens),
            calculate_order_book_imbalance(conn, puts_tokens)
        )
        
        # Unpack results with explicit type hints
        futures_cvd: float = results[0]  # type: ignore
        calls_cvd: float = results[1]  # type: ignore
        puts_cvd: float = results[2]  # type: ignore
        
        futures_volume: float
        futures_oi_change: float
        futures_volume, futures_oi_change = results[3]  # type: ignore
        
        calls_volume: float
        calls_oi_change: float
        calls_volume, calls_oi_change = results[4]  # type: ignore
        
        puts_volume: float
        puts_oi_change: float
        puts_volume, puts_oi_change = results[5]  # type: ignore
        
        futures_bid_value: float
        futures_ask_value: float
        futures_bid_value, futures_ask_value = results[6]  # type: ignore
        
        calls_bid_value: float
        calls_ask_value: float
        calls_bid_value, calls_ask_value = results[7]  # type: ignore
        
        puts_bid_value: float
        puts_ask_value: float
        puts_bid_value, puts_ask_value = results[8]  # type: ignore
        
        # Construct response
        response = {
            "market_overview": {
                "symbol": "NIFTY",
                "price": round(current_price, 2),
                "change": round(price_change, 2),
                "change_pct": round(price_change_pct, 2)
            },
            "cvd": {
                "futures": round(futures_cvd, 2),
                "calls": round(calls_cvd, 2),
                "puts": round(puts_cvd, 2),
                "net_options": round(calls_cvd - puts_cvd, 2)
            },
            "volume": {
                "futures": round(futures_volume, 2),
                "calls": round(calls_volume, 2),
                "puts": round(puts_volume, 2),
                "total": round(futures_volume + calls_volume + puts_volume, 2)
            },
            "oi_changes": {
                "futures": round(futures_oi_change, 2),
                "calls": round(calls_oi_change, 2),
                "puts": round(puts_oi_change, 2),
                "total": round(futures_oi_change + calls_oi_change + puts_oi_change, 2)
            },
            "imbalance": {
                "futures": {
                    "bid_value": round(futures_bid_value, 2),
                    "ask_value": round(futures_ask_value, 2),
                    "ratio": round(futures_bid_value / futures_ask_value if futures_ask_value > 0 else 0, 2)
                },
                "calls": {
                    "bid_value": round(calls_bid_value, 2),
                    "ask_value": round(calls_ask_value, 2),
                    "ratio": round(calls_bid_value / calls_ask_value if calls_ask_value > 0 else 0, 2)
                },
                "puts": {
                    "bid_value": round(puts_bid_value, 2),
                    "ask_value": round(puts_ask_value, 2),
                    "ratio": round(puts_bid_value / puts_ask_value if puts_ask_value > 0 else 0, 2)
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
