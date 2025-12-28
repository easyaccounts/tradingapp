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


@router.get("/orderflow/toxicity")
async def get_toxicity_metrics(
    limit: int = Query(500, description="Number of recent ticks to analyze", ge=100, le=2000)
):
    """
    Get orderflow toxicity metrics for the last N ticks across NIFTY instruments
    
    Toxicity metrics help identify high-probability price moves by detecting:
    - Informed trading (toxic flow)
    - Market maker stress
    - Liquidity consumption patterns
    
    Args:
        limit: Number of recent ticks to analyze (default 500)
    
    Returns:
    - avg_toxicity: Average toxicity metrics by instrument type
    - high_toxicity_events: Ticks with extreme toxicity (top 10%)
    - toxicity_trend: Recent trend direction
    """
    
    conn = await get_db_connection()
    redis_client = get_redis_client()
    
    try:
        # Get NIFTY instrument tokens (2 nearest expiries)
        futures_tokens, calls_tokens, puts_tokens = get_nifty_instruments_by_expiry(redis_client)
        
        if not futures_tokens:
            redis_client.close()
            raise HTTPException(status_code=404, detail="No NIFTY futures found")
        
        # Combine all tokens
        all_tokens = futures_tokens + calls_tokens + puts_tokens
        
        # Get last N ticks with toxicity metrics
        toxicity_data = await conn.fetch("""
            SELECT 
                instrument_token,
                time,
                consumption_rate,
                flow_intensity,
                depth_toxicity_tick,
                kyle_lambda_tick,
                last_price,
                volume_delta,
                cvd_change
            FROM ticks
            WHERE instrument_token = ANY($1)
                AND consumption_rate IS NOT NULL
                AND depth_toxicity_tick IS NOT NULL
                AND kyle_lambda_tick IS NOT NULL
            ORDER BY time DESC
            LIMIT $2
        """, all_tokens, limit)
        
        if not toxicity_data:
            raise HTTPException(status_code=404, detail="No toxicity data available")
        
        # Categorize by instrument type
        futures_metrics = []
        calls_metrics = []
        puts_metrics = []
        
        for row in toxicity_data:
            token = row['instrument_token']
            metrics = {
                'token': token,
                'time': row['time'].isoformat(),
                'consumption_rate': float(row['consumption_rate'] or 0),
                'flow_intensity': float(row['flow_intensity'] or 0),
                'depth_toxicity': float(row['depth_toxicity_tick'] or 0),
                'kyle_lambda': float(row['kyle_lambda_tick'] or 0),
                'price': float(row['last_price'] or 0),
                'volume_delta': float(row['volume_delta'] or 0),
                'cvd_change': float(row['cvd_change'] or 0)
            }
            
            if token in futures_tokens:
                futures_metrics.append(metrics)
            elif token in calls_tokens:
                calls_metrics.append(metrics)
            elif token in puts_tokens:
                puts_metrics.append(metrics)
        
        # Calculate averages
        def calc_avg(metrics_list):
            if not metrics_list:
                return {
                    'consumption_rate': 0,
                    'flow_intensity': 0,
                    'depth_toxicity': 0,
                    'kyle_lambda': 0,
                    'count': 0
                }
            return {
                'consumption_rate': round(sum(m['consumption_rate'] for m in metrics_list) / len(metrics_list), 4),
                'flow_intensity': round(sum(m['flow_intensity'] for m in metrics_list) / len(metrics_list), 6),
                'depth_toxicity': round(sum(m['depth_toxicity'] for m in metrics_list) / len(metrics_list), 4),
                'kyle_lambda': round(sum(m['kyle_lambda'] for m in metrics_list) / len(metrics_list), 6),
                'count': len(metrics_list)
            }
        
        # Identify high toxicity events (top 10% by kyle_lambda)
        all_metrics = futures_metrics + calls_metrics + puts_metrics
        all_metrics_sorted = sorted(all_metrics, key=lambda x: x['kyle_lambda'], reverse=True)
        high_toxicity_threshold = int(len(all_metrics_sorted) * 0.1)
        high_toxicity_events = all_metrics_sorted[:high_toxicity_threshold][:20]  # Top 20 for display
        
        # Calculate recent trend (last 100 ticks vs previous)
        recent_100 = all_metrics_sorted[:100] if len(all_metrics_sorted) >= 100 else all_metrics_sorted
        prev_100 = all_metrics_sorted[100:200] if len(all_metrics_sorted) >= 200 else []
        
        recent_avg_toxicity = sum(m['kyle_lambda'] for m in recent_100) / len(recent_100) if recent_100 else 0
        prev_avg_toxicity = sum(m['kyle_lambda'] for m in prev_100) / len(prev_100) if prev_100 else recent_avg_toxicity
        
        toxicity_change = recent_avg_toxicity - prev_avg_toxicity
        toxicity_trend = "INCREASING" if toxicity_change > 0.0001 else "DECREASING" if toxicity_change < -0.0001 else "STABLE"
        
        # Calculate per-instrument rankings
        # Get latest toxicity per instrument with trend calculation
        instrument_rankings = await conn.fetch("""
            WITH latest_ticks AS (
                SELECT DISTINCT ON (instrument_token)
                    instrument_token,
                    time,
                    kyle_lambda_tick,
                    depth_toxicity_tick,
                    flow_intensity,
                    consumption_rate,
                    last_price,
                    volume_delta,
                    cvd_change
                FROM ticks
                WHERE instrument_token = ANY($1)
                    AND kyle_lambda_tick IS NOT NULL
                    AND time >= NOW() - INTERVAL '5 minutes'
                ORDER BY instrument_token, time DESC
            ),
            previous_ticks AS (
                SELECT DISTINCT ON (instrument_token)
                    instrument_token,
                    kyle_lambda_tick as prev_kyle_lambda
                FROM ticks
                WHERE instrument_token = ANY($1)
                    AND kyle_lambda_tick IS NOT NULL
                    AND time >= NOW() - INTERVAL '10 minutes'
                    AND time < NOW() - INTERVAL '5 minutes'
                ORDER BY instrument_token, time DESC
            )
            SELECT 
                l.instrument_token,
                l.kyle_lambda_tick,
                l.depth_toxicity_tick,
                l.flow_intensity,
                l.consumption_rate,
                l.last_price,
                l.volume_delta,
                l.cvd_change,
                l.time,
                COALESCE(p.prev_kyle_lambda, l.kyle_lambda_tick) as prev_kyle_lambda
            FROM latest_ticks l
            LEFT JOIN previous_ticks p ON l.instrument_token = p.instrument_token
            ORDER BY l.kyle_lambda_tick DESC
            LIMIT 20
        """, all_tokens)
        
        # Enrich with instrument details from Redis
        ranked_instruments = []
        for row in instrument_rankings:
            token = row['instrument_token']
            instrument_key = f"instrument:{token}"
            instrument_data: dict = redis_client.hgetall(instrument_key)  # type: ignore
            
            if not instrument_data:
                continue
            
            current_kyle = float(row['kyle_lambda_tick'] or 0)
            prev_kyle = float(row['prev_kyle_lambda'] or current_kyle)
            trend_change = current_kyle - prev_kyle
            trend_direction = "↑" if trend_change > 0.00001 else "↓" if trend_change < -0.00001 else "→"
            
            # Determine instrument type
            inst_type = "FUTURES" if token in futures_tokens else "CALLS" if token in calls_tokens else "PUTS"
            
            ranked_instruments.append({
                "rank": len(ranked_instruments) + 1,
                "trading_symbol": instrument_data.get('tradingsymbol', 'N/A'),
                "instrument_type": inst_type,
                "strike": float(instrument_data.get('strike', 0)) if instrument_data.get('strike') else None,
                "expiry": instrument_data.get('expiry', 'N/A').split()[0] if instrument_data.get('expiry') else 'N/A',
                "kyle_lambda": round(current_kyle, 6),
                "depth_toxicity": round(float(row['depth_toxicity_tick'] or 0), 4),
                "flow_intensity": round(float(row['flow_intensity'] or 0), 6),
                "consumption_rate": round(float(row['consumption_rate'] or 0), 4),
                "price": round(float(row['last_price'] or 0), 2),
                "trend": trend_direction,
                "trend_change": round(trend_change, 6),
                "cvd_change": round(float(row['cvd_change'] or 0), 2),
                "last_updated": row['time'].isoformat()
            })
        
        # Close Redis connection
        redis_client.close()
        
        response = {
            "avg_toxicity": {
                "futures": calc_avg(futures_metrics),
                "calls": calc_avg(calls_metrics),
                "puts": calc_avg(puts_metrics),
                "overall": calc_avg(all_metrics)
            },
            "high_toxicity_events": [
                {
                    "time": evt['time'],
                    "kyle_lambda": round(evt['kyle_lambda'], 6),
                    "depth_toxicity": round(evt['depth_toxicity'], 4),
                    "flow_intensity": round(evt['flow_intensity'], 6),
                    "price": round(evt['price'], 2),
                    "cvd_change": round(evt['cvd_change'], 2),
                    "type": "FUTURES" if evt['token'] in futures_tokens else "CALLS" if evt['token'] in calls_tokens else "PUTS"
                } for evt in high_toxicity_events
            ],
            "toxicity_trend": {
                "direction": toxicity_trend,
                "recent_avg": round(recent_avg_toxicity, 6),
                "previous_avg": round(prev_avg_toxicity, 6),
                "change": round(toxicity_change, 6)
            },
            "ranked_instruments": ranked_instruments,
            "metadata": {
                "ticks_analyzed": len(all_metrics),
                "limit_requested": limit,
                "futures_ticks": len(futures_metrics),
                "calls_ticks": len(calls_metrics),
                "puts_ticks": len(puts_metrics)
            }
        }
        
        return response
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Toxicity analysis failed: {str(e)}")
    
    finally:
        await conn.close()
