from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timedelta
import asyncpg
from redis import Redis
import os
from typing import Optional

router = APIRouter()

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
    
    # asyncpg can directly use postgresql:// URLs
    # Convert postgres:// to postgresql:// if needed
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    
    try:
        return await asyncpg.connect(database_url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")


@router.get("/orderflow")
async def get_orderflow_analysis(
    instrument_token: int = Query(..., description="Instrument token to analyze")
):
    """
    Get comprehensive orderflow analysis for a given instrument
    
    Returns:
    - market_overview: Current price, volume, OI, day range
    - trade_signal: LONG/SHORT/NEUTRAL based on 5 signals
    - cvd: Cumulative Volume Delta (3m, 15m)
    - imbalance: Bid/Ask ratio analysis
    - oi_pattern: Open Interest pattern analysis
    - vwap: VWAP position and deviation
    - volume: Recent volume analysis (last 5 min)
    - order_book: Top 5 bid/ask levels
    """
    
    conn = await get_db_connection()
    
    try:
        # Time windows
        now = datetime.utcnow()
        time_3m = now - timedelta(minutes=3)
        time_15m = now - timedelta(minutes=15)
        time_5m = now - timedelta(minutes=5)
        time_1m = now - timedelta(minutes=1)
        
        # 1. Get latest tick for current market data
        latest = await conn.fetchrow("""
            SELECT 
                time,
                instrument_token,
                last_price,
                volume_traded,
                oi,
                total_buy_quantity,
                total_sell_quantity,
                average_traded_price,
                bid_prices,
                bid_quantities,
                bid_orders,
                ask_prices,
                ask_quantities,
                ask_orders
            FROM ticks
            WHERE instrument_token = $1
            ORDER BY time DESC
            LIMIT 1
        """, instrument_token)
        
        if not latest:
            raise HTTPException(status_code=404, detail=f"No data found for instrument {instrument_token}")
        
        # 2. Calculate CVD (full day) using volume delta with aggressor side
        cvd_data = await conn.fetch("""
            SELECT 
                time,
                volume_traded,
                last_price,
                bid_prices,
                ask_prices,
                total_buy_quantity,
                total_sell_quantity
            FROM ticks
            WHERE instrument_token = $1 
                AND time >= DATE_TRUNC('day', NOW())
            ORDER BY time ASC
        """, instrument_token)
        
        # Calculate CVD using volume delta with aggressor side detection
        cvd_day = 0.0
        prev_volume = None
        
        for row in cvd_data:
            current_volume = float(row['volume_traded'] or 0)
            last_price = float(row['last_price'] or 0)
            bid_prices = row['bid_prices'] or []
            ask_prices = row['ask_prices'] or []
            
            if prev_volume is not None and current_volume > prev_volume and last_price > 0:
                volume_delta = float(current_volume - prev_volume)
                
                # Determine aggressor side by comparing last price with bid/ask
                best_bid = float(bid_prices[0]) if bid_prices and len(bid_prices) > 0 and bid_prices[0] else 0
                best_ask = float(ask_prices[0]) if ask_prices and len(ask_prices) > 0 and ask_prices[0] else 0
                
                # If trade at ask or above, buyer is aggressor (bullish)
                # If trade at bid or below, seller is aggressor (bearish)
                if best_ask > 0 and last_price >= best_ask:
                    cvd_change = volume_delta  # Buyer initiated
                elif best_bid > 0 and last_price <= best_bid:
                    cvd_change = -volume_delta  # Seller initiated
                else:
                    # Trade between bid/ask, use mid-price comparison
                    if best_bid > 0 and best_ask > 0:
                        mid_price = (best_bid + best_ask) / 2
                        if last_price > mid_price:
                            cvd_change = volume_delta  # Closer to ask = buyer aggressor
                        else:
                            cvd_change = -volume_delta  # Closer to bid = seller aggressor
                    else:
                        cvd_change = 0  # Can't determine, skip
                
                cvd_day += cvd_change
            
            prev_volume = current_volume if current_volume > 0 else prev_volume
        
        # 2B. Calculate Segmented CVD (Futures + Options for 2 nearest expiries)
        # Get instrument tokens for NIFTY options with 2 nearest expiries from Redis
        redis_client = get_redis_client()
        
        # Get all NIFTY option tokens from Redis (single scan)
        expiry_map = {}  # Map token to expiry date
        call_tokens = []
        put_tokens = []
        
        for key in redis_client.scan_iter("instrument:*"):
            token = key.split(":")[-1]
            instrument_data: dict = redis_client.hgetall(key)  # type: ignore
            
            segment = instrument_data.get('segment', '')
            name = instrument_data.get('name', '')
            tradingsymbol = instrument_data.get('tradingsymbol', '')
            expiry_str = instrument_data.get('expiry', '')
            
            # Filter for NIFTY options (NFO-OPT segment, NIFTY name)
            if segment == 'NFO-OPT' and name == 'NIFTY' and expiry_str:
                try:
                    # Parse expiry date
                    if expiry_str and expiry_str != 'None':
                        expiry_date = datetime.strptime(expiry_str.split()[0], '%Y-%m-%d').date()
                        token_int = int(token)
                        expiry_map[token_int] = expiry_date
                        
                        # Categorize as call or put based on symbol
                        if tradingsymbol.endswith('CE'):
                            call_tokens.append((token_int, expiry_date))
                        elif tradingsymbol.endswith('PE'):
                            put_tokens.append((token_int, expiry_date))
                except:
                    continue
        
        redis_client.close()
        
        # Get 2 nearest expiries
        unique_expiries = sorted(set(expiry_map.values()))[:2]
        
        # Filter tokens to only those with 2 nearest expiries
        filtered_call_tokens = [token for token, expiry in call_tokens if expiry in unique_expiries]
        filtered_put_tokens = [token for token, expiry in put_tokens if expiry in unique_expiries]
        
        calls_cvd_day = 0.0
        puts_cvd_day = 0.0
        
        if filtered_call_tokens:
            # Query NIFTY Call options for 2 nearest expiries
            calls_data = await conn.fetch("""
                SELECT 
                    time,
                    volume_traded,
                    last_price,
                    bid_prices,
                    ask_prices,
                    total_buy_quantity,
                    total_sell_quantity
                FROM ticks
                WHERE instrument_token = ANY($1)
                    AND time >= DATE_TRUNC('day', NOW())
                ORDER BY time ASC
            """, filtered_call_tokens)
        else:
            calls_data = []
        
        if filtered_put_tokens:
            # Query NIFTY Put options for 2 nearest expiries
            puts_data = await conn.fetch("""
                SELECT 
                    time,
                    volume_traded,
                    last_price,
                    bid_prices,
                    ask_prices,
                    total_buy_quantity,
                    total_sell_quantity
                FROM ticks
                WHERE instrument_token = ANY($1)
                    AND time >= DATE_TRUNC('day', NOW())
                ORDER BY time ASC
            """, filtered_put_tokens)
        else:
            puts_data = []
        
        calls_cvd_day = 0.0
        puts_cvd_day = 0.0
        
        # Process Calls
        prev_vol = {}
        for row in calls_data:
            vol = float(row['volume_traded'] or 0)
            price = float(row['last_price'] or 0)
            
            # Use some unique key per strike - simplified to use volume as proxy
            if vol > 0 and price > 0:
                # Calculate CVD change similar to futures
                bid_prices = row['bid_prices'] or []
                ask_prices = row['ask_prices'] or []
                best_bid = float(bid_prices[0]) if bid_prices and len(bid_prices) > 0 and bid_prices[0] else 0
                best_ask = float(ask_prices[0]) if ask_prices and len(ask_prices) > 0 and ask_prices[0] else 0
                
                # Simplified: accumulate based on aggressor side
                if best_ask > 0 and price >= best_ask:
                    cvd_inc = 1  # Buyer aggressor
                elif best_bid > 0 and price <= best_bid:
                    cvd_inc = -1  # Seller aggressor
                else:
                    # Use mid-price comparison
                    if best_bid > 0 and best_ask > 0:
                        mid_price = (best_bid + best_ask) / 2
                        cvd_inc = 1 if price > mid_price else -1
                    else:
                        cvd_inc = 0
                
                calls_cvd_day += cvd_inc
        
        # Process Puts
        for row in puts_data:
            vol = float(row['volume_traded'] or 0)
            price = float(row['last_price'] or 0)
            
            if vol > 0 and price > 0:
                bid_prices = row['bid_prices'] or []
                ask_prices = row['ask_prices'] or []
                best_bid = float(bid_prices[0]) if bid_prices and len(bid_prices) > 0 and bid_prices[0] else 0
                best_ask = float(ask_prices[0]) if ask_prices and len(ask_prices) > 0 and ask_prices[0] else 0
                
                if best_ask > 0 and price >= best_ask:
                    cvd_inc = 1
                elif best_bid > 0 and price <= best_bid:
                    cvd_inc = -1
                else:
                    # Use mid-price comparison
                    if best_bid > 0 and best_ask > 0:
                        mid_price = (best_bid + best_ask) / 2
                        cvd_inc = 1 if price > mid_price else -1
                    else:
                        cvd_inc = 0
                
                puts_cvd_day += cvd_inc
        
        # Calculate net options CVD (ensure proper types)
        net_options_day = float(calls_cvd_day) - float(puts_cvd_day)
        
        # 3. Calculate Imbalance (current + 30s avg)
        imbalance_data = await conn.fetch("""
            SELECT 
                total_buy_quantity,
                total_sell_quantity
            FROM ticks
            WHERE instrument_token = $1 
                AND time >= NOW() - INTERVAL '30 seconds'
            ORDER BY time DESC
        """, instrument_token)
        
        current_bid = float(latest['total_buy_quantity'] or 0)
        current_ask = float(latest['total_sell_quantity'] or 0)
        current_ratio = float(current_bid / current_ask) if current_ask > 0 else 0.0
        
        # Calculate 30s average ratio
        avg_ratios = []
        for row in imbalance_data:
            bid = float(row['total_buy_quantity'] or 0)
            ask = float(row['total_sell_quantity'] or 0)
            if ask > 0:
                avg_ratios.append(float(bid / ask))
        
        avg_ratio_30s = float(sum(avg_ratios) / len(avg_ratios)) if avg_ratios else current_ratio
        
        # 4. OI Pattern Analysis
        oi_1m_ago = await conn.fetchval("""
            SELECT oi
            FROM ticks
            WHERE instrument_token = $1 
                AND time <= $2
            ORDER BY time DESC
            LIMIT 1
        """, instrument_token, time_1m)
        
        current_oi = float(latest['oi'] or 0)
        oi_change = float(current_oi - float(oi_1m_ago or current_oi))
        current_price = float(latest['last_price'] or 0)
        
        # Get price from 1m ago for comparison
        price_1m_ago = await conn.fetchval("""
            SELECT last_price
            FROM ticks
            WHERE instrument_token = $1 
                AND time <= $2
            ORDER BY time DESC
            LIMIT 1
        """, instrument_token, time_1m)
        
        price_change = float(current_price - float(price_1m_ago or current_price))
        
        # Determine OI pattern
        if oi_change > 0 and price_change > 0:
            oi_pattern = "LONG BUILDUP"
            oi_interpretation = "Strong Bullish"
        elif oi_change > 0 and price_change < 0:
            oi_pattern = "SHORT BUILDUP"
            oi_interpretation = "Strong Bearish"
        elif oi_change < 0 and price_change > 0:
            oi_pattern = "SHORT COVERING"
            oi_interpretation = "Weak Bullish"
        elif oi_change < 0 and price_change < 0:
            oi_pattern = "LONG UNWINDING"
            oi_interpretation = "Weak Bearish"
        else:
            oi_pattern = "CONSOLIDATION"
            oi_interpretation = "Neutral"
        
        # 5. VWAP Calculation
        vwap_data = await conn.fetchrow("""
            SELECT 
                SUM(last_price * volume_traded) / NULLIF(SUM(volume_traded), 0) as vwap
            FROM ticks
            WHERE instrument_token = $1 
                AND time >= DATE_TRUNC('day', NOW())
        """, instrument_token)
        
        vwap = vwap_data['vwap'] or current_price
        vwap = float(vwap) if vwap else float(current_price)
        vwap_deviation = float(current_price - vwap)
        vwap_deviation_pct = float(vwap_deviation / vwap * 100) if vwap > 0 else 0.0
        vwap_position = "ABOVE" if current_price >= vwap else "BELOW"
        
        # 6. Volume Analysis (last 5 minutes, 1-min buckets)
        volume_data = await conn.fetch("""
            SELECT 
                time_bucket('1 minute', time) as bucket,
                MAX(volume_traded) - MIN(volume_traded) as volume_change
            FROM ticks
            WHERE instrument_token = $1 
                AND time >= $2
            GROUP BY bucket
            ORDER BY bucket DESC
            LIMIT 5
        """, instrument_token, time_5m)
        
        volumes = [float(row['volume_change'] or 0) for row in reversed(volume_data)]
        times = [row['bucket'].strftime('%H:%M') for row in reversed(volume_data)]
        avg_volume = float(sum(volumes) / len(volumes)) if volumes else 0.0
        
        # 7. Order Book (top 5 levels)
        bid_prices = latest['bid_prices'] or []
        bid_quantities = latest['bid_quantities'] or []
        bid_orders = latest['bid_orders'] or []
        ask_prices = latest['ask_prices'] or []
        ask_quantities = latest['ask_quantities'] or []
        ask_orders = latest['ask_orders'] or []
        
        # Calculate spread
        spread = 0
        if bid_prices and ask_prices and len(bid_prices) > 0 and len(ask_prices) > 0:
            best_bid = float(bid_prices[0]) if bid_prices[0] is not None else 0.0
            best_ask = float(ask_prices[0]) if ask_prices[0] is not None else 0.0
            spread = float(best_ask - best_bid) if best_ask > best_bid else 0.0
        
        # 8. Market Overview (day range)
        day_stats = await conn.fetchrow("""
            SELECT 
                MAX(last_price) as day_high,
                MIN(last_price) as day_low,
                (SELECT last_price FROM ticks 
                 WHERE instrument_token = $1 
                 AND time >= DATE_TRUNC('day', NOW())
                 ORDER BY time ASC LIMIT 1) as day_open
            FROM ticks
            WHERE instrument_token = $1 
                AND time >= DATE_TRUNC('day', NOW())
        """, instrument_token)
        
        day_high = float(day_stats['day_high'] or current_price)
        day_low = float(day_stats['day_low'] or current_price)
        day_open = float(day_stats['day_open'] or current_price)
        day_range = float(day_high - day_low)
        price_change_day = float(current_price - day_open)
        price_change_pct = float(price_change_day / day_open * 100) if day_open > 0 else 0.0
        
        # 9. Generate Trade Signal
        signals = []
        
        # Signal 1: CVD (day)
        if cvd_day > 0:
            signals.append(1)  # Bullish
        elif cvd_day < 0:
            signals.append(-1)  # Bearish
        else:
            signals.append(0)  # Neutral
        
        # Signal 2: Imbalance
        if current_ratio > 1.5:
            signals.append(1)  # Bullish
        elif current_ratio < 0.67:
            signals.append(-1)  # Bearish
        else:
            signals.append(0)
        
        # Signal 3: OI Pattern
        if oi_pattern in ["LONG BUILDUP", "SHORT COVERING"]:
            signals.append(1)
        elif oi_pattern in ["SHORT BUILDUP", "LONG UNWINDING"]:
            signals.append(-1)
        else:
            signals.append(0)
        
        # Signal 4: VWAP
        if vwap_position == "ABOVE":
            signals.append(1)
        else:
            signals.append(-1)
        
        # Signal 5: Volume Spike
        recent_volume = volumes[-1] if volumes else 0
        if recent_volume > avg_volume * 1.5:
            # Volume spike - amplify current trend
            if sum(signals) > 0:
                signals.append(1)
            elif sum(signals) < 0:
                signals.append(-1)
            else:
                signals.append(0)
        else:
            signals.append(0)
        
        # Aggregate signals
        signal_sum = sum(signals)
        bullish_count = sum(1 for s in signals if s > 0)
        bearish_count = sum(1 for s in signals if s < 0)
        
        if signal_sum >= 3 and bullish_count >= 4:
            trade_signal = "LONG"
            trade_description = f"Strong bullish setup: {bullish_count}/5 signals confirm uptrend"
        elif signal_sum <= -3 and bearish_count >= 4:
            trade_signal = "SHORT"
            trade_description = f"Strong bearish setup: {bearish_count}/5 signals confirm downtrend"
        else:
            trade_signal = "NEUTRAL"
            trade_description = f"Mixed signals: {bullish_count} bullish, {bearish_count} bearish - wait for clarity"
        
        # Construct response
        response = {
            "market_overview": {
                "symbol": "NIFTY FUT",
                "price": float(current_price),
                "change": float(price_change_day),
                "change_pct": round(float(price_change_pct), 2),
                "volume": int(latest['volume_traded'] or 0),
                "oi": int(current_oi),
                "day_range": round(float(day_range), 2)
            },
            "trade_signal": {
                "signal": trade_signal,
                "description": trade_description,
                "bullish_signals": bullish_count,
                "bearish_signals": bearish_count
            },
            "cvd": {
                "cvd_day": round(float(cvd_day), 2)
            },
            "cvd_segmented": {
                "futures": {
                    "cvd_day": round(float(cvd_day), 2)
                },
                "calls": {
                    "cvd_day": round(float(calls_cvd_day), 2)
                },
                "puts": {
                    "cvd_day": round(float(puts_cvd_day), 2)
                },
                "net_options": {
                    "cvd_day": round(float(net_options_day), 2)
                }
            },
            "imbalance": {
                "ratio": round(float(current_ratio), 2),
                "bid_qty": int(current_bid),
                "ask_qty": int(current_ask),
                "avg_ratio_30s": round(float(avg_ratio_30s), 2)
            },
            "oi_pattern": {
                "oi": int(current_oi),
                "oi_change": int(oi_change),
                "pattern": oi_pattern,
                "interpretation": oi_interpretation
            },
            "vwap": {
                "price": float(current_price),
                "vwap": round(float(vwap), 2),
                "deviation": round(float(vwap_deviation), 2),
                "deviation_pct": round(float(vwap_deviation_pct), 2),
                "position": vwap_position
            },
            "volume": {
                "volumes": volumes,
                "times": times,
                "avg_volume": round(float(avg_volume), 2)
            },
            "order_book": {
                "bid_prices": [float(p) for p in bid_prices],
                "bid_quantities": [int(q) for q in bid_quantities],
                "bid_orders": [int(o) for o in bid_orders],
                "ask_prices": [float(p) for p in ask_prices],
                "ask_quantities": [int(q) for q in ask_quantities],
                "ask_orders": [int(o) for o in ask_orders],
                "spread": round(float(spread), 2)
            }
        }
        
        return response
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")
    
    finally:
        await conn.close()


@router.get("/orderflow/history")
async def get_historical_analysis(
    instrument_token: int = Query(..., description="Instrument token to analyze")
):
    """
    Get intraday historical analysis with time-series data
    
    Returns:
    - price_action: 5-min candles throughout the day
    - volume_profile: Volume distribution by price level
    - oi_evolution: OI changes over time
    - key_levels: Support/resistance based on volume
    """
    
    conn = await get_db_connection()
    
    try:
        # 1. Get 5-minute price candles for the day
        candles = await conn.fetch("""
            SELECT 
                time_bucket('5 minutes', time) as bucket,
                FIRST(last_price, time) as open,
                MAX(last_price) as high,
                MIN(last_price) as low,
                LAST(last_price, time) as close,
                MAX(volume_traded) - MIN(volume_traded) as volume,
                AVG(oi) as avg_oi
            FROM ticks
            WHERE instrument_token = $1 
                AND time >= DATE_TRUNC('day', NOW())
            GROUP BY bucket
            ORDER BY bucket ASC
        """, instrument_token)
        
        price_action = []
        for candle in candles:
            price_action.append({
                "time": candle['bucket'].strftime('%H:%M'),
                "open": float(candle['open'] or 0),
                "high": float(candle['high'] or 0),
                "low": float(candle['low'] or 0),
                "close": float(candle['close'] or 0),
                "volume": int(candle['volume'] or 0),
                "oi": int(candle['avg_oi'] or 0)
            })
        
        # 2. Volume Profile - distribution by price level
        volume_profile = await conn.fetch("""
            SELECT 
                ROUND(last_price / 10) * 10 as price_level,
                COUNT(*) as tick_count,
                SUM(COALESCE(volume_traded, 0)) as total_volume
            FROM ticks
            WHERE instrument_token = $1 
                AND time >= DATE_TRUNC('day', NOW())
                AND last_price IS NOT NULL
            GROUP BY price_level
            ORDER BY total_volume DESC
            LIMIT 20
        """, instrument_token)
        
        volume_by_price = []
        for row in volume_profile:
            volume_by_price.append({
                "price_level": float(row['price_level'] or 0),
                "volume": int(row['total_volume'] or 0),
                "tick_count": int(row['tick_count'] or 0)
            })
        
        # 3. OI Evolution - hourly OI changes
        oi_evolution = await conn.fetch("""
            SELECT 
                time_bucket('30 minutes', time) as bucket,
                AVG(oi) as avg_oi,
                AVG(last_price) as avg_price
            FROM ticks
            WHERE instrument_token = $1 
                AND time >= DATE_TRUNC('day', NOW())
            GROUP BY bucket
            ORDER BY bucket ASC
        """, instrument_token)
        
        oi_timeline = []
        for row in oi_evolution:
            oi_timeline.append({
                "time": row['bucket'].strftime('%H:%M'),
                "oi": int(row['avg_oi'] or 0),
                "price": float(row['avg_price'] or 0)
            })
        
        # 4. Key Price Levels - support/resistance based on volume
        key_levels = []
        if volume_by_price:
            # Top 5 price levels with highest volume
            sorted_levels = sorted(volume_by_price, key=lambda x: x['volume'], reverse=True)[:5]
            for level in sorted_levels:
                key_levels.append({
                    "price": level['price_level'],
                    "volume": level['volume'],
                    "type": "High Volume Node"
                })
        
        # 5. Day Statistics
        day_stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total_ticks,
                MAX(volume_traded) as peak_volume,
                MAX(oi) as peak_oi,
                MIN(oi) as min_oi,
                STDDEV(last_price) as price_volatility
            FROM ticks
            WHERE instrument_token = $1 
                AND time >= DATE_TRUNC('day', NOW())
        """, instrument_token)
        
        response = {
            "price_action": price_action,
            "volume_profile": volume_by_price,
            "oi_evolution": oi_timeline,
            "key_levels": key_levels,
            "day_stats": {
                "total_ticks": int(day_stats['total_ticks'] or 0),
                "peak_volume": int(day_stats['peak_volume'] or 0),
                "peak_oi": int(day_stats['peak_oi'] or 0),
                "min_oi": int(day_stats['min_oi'] or 0),
                "price_volatility": round(float(day_stats['price_volatility'] or 0), 2)
            }
        }
        
        return response
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Historical analysis failed: {str(e)}")
    
    finally:
        await conn.close()
