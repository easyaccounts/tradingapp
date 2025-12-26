from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timedelta
import asyncpg
import os
from typing import Optional
from urllib.parse import urlparse

router = APIRouter()

# Database connection
async def get_db_connection():
    """Create database connection from DATABASE_URL"""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")
    
    # Parse the DATABASE_URL
    parsed = urlparse(database_url)
    
    return await asyncpg.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        user=parsed.username,
        password=parsed.password,
        database=parsed.path.lstrip('/')
    )


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
                depth_buy_price,
                depth_buy_quantity,
                depth_buy_orders,
                depth_sell_price,
                depth_sell_quantity,
                depth_sell_orders
            FROM ticks
            WHERE instrument_token = $1
            ORDER BY time DESC
            LIMIT 1
        """, instrument_token)
        
        if not latest:
            raise HTTPException(status_code=404, detail=f"No data found for instrument {instrument_token}")
        
        # 2. Calculate CVD (3-min and 15-min)
        cvd_data = await conn.fetch("""
            SELECT 
                time,
                volume_traded,
                total_buy_quantity,
                total_sell_quantity,
                last_price
            FROM ticks
            WHERE instrument_token = $1 
                AND time >= $2
            ORDER BY time ASC
        """, instrument_token, time_15m)
        
        # Calculate CVD
        cvd_3m = 0
        cvd_15m = 0
        prev_volume = None
        
        for row in cvd_data:
            if prev_volume is not None:
                volume_delta = row['volume_traded'] - prev_volume
                
                # Use buy/sell pressure to determine direction
                if row['total_buy_quantity'] > row['total_sell_quantity']:
                    cvd_change = volume_delta
                else:
                    cvd_change = -volume_delta
                
                cvd_15m += cvd_change
                
                # Only add to 3m if within window
                if row['time'] >= time_3m:
                    cvd_3m += cvd_change
            
            prev_volume = row['volume_traded']
        
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
        
        current_bid = latest['total_buy_quantity'] or 0
        current_ask = latest['total_sell_quantity'] or 0
        current_ratio = current_bid / current_ask if current_ask > 0 else 0
        
        # Calculate 30s average ratio
        avg_ratios = []
        for row in imbalance_data:
            bid = row['total_buy_quantity'] or 0
            ask = row['total_sell_quantity'] or 0
            if ask > 0:
                avg_ratios.append(bid / ask)
        
        avg_ratio_30s = sum(avg_ratios) / len(avg_ratios) if avg_ratios else current_ratio
        
        # 4. OI Pattern Analysis
        oi_1m_ago = await conn.fetchval("""
            SELECT oi
            FROM ticks
            WHERE instrument_token = $1 
                AND time <= $2
            ORDER BY time DESC
            LIMIT 1
        """, instrument_token, time_1m)
        
        current_oi = latest['oi'] or 0
        oi_change = current_oi - (oi_1m_ago or current_oi)
        current_price = latest['last_price']
        
        # Get price from 1m ago for comparison
        price_1m_ago = await conn.fetchval("""
            SELECT last_price
            FROM ticks
            WHERE instrument_token = $1 
                AND time <= $2
            ORDER BY time DESC
            LIMIT 1
        """, instrument_token, time_1m)
        
        price_change = current_price - (price_1m_ago or current_price)
        
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
        vwap_deviation = current_price - vwap
        vwap_deviation_pct = (vwap_deviation / vwap * 100) if vwap > 0 else 0
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
        
        volumes = [row['volume_change'] for row in reversed(volume_data)]
        times = [row['bucket'].strftime('%H:%M') for row in reversed(volume_data)]
        avg_volume = sum(volumes) / len(volumes) if volumes else 0
        
        # 7. Order Book (top 5 levels)
        depth_buy_prices = latest['depth_buy_price'] or []
        depth_buy_quantities = latest['depth_buy_quantity'] or []
        depth_buy_orders = latest['depth_buy_orders'] or []
        depth_sell_prices = latest['depth_sell_price'] or []
        depth_sell_quantities = latest['depth_sell_quantity'] or []
        depth_sell_orders = latest['depth_sell_orders'] or []
        
        # Take top 5 levels
        bid_prices = depth_buy_prices[:5] if depth_buy_prices else []
        bid_quantities = depth_buy_quantities[:5] if depth_buy_quantities else []
        bid_orders = depth_buy_orders[:5] if depth_buy_orders else []
        ask_prices = depth_sell_prices[:5] if depth_sell_prices else []
        ask_quantities = depth_sell_quantities[:5] if depth_sell_quantities else []
        ask_orders = depth_sell_orders[:5] if depth_sell_orders else []
        
        # Calculate spread
        spread = 0
        if bid_prices and ask_prices:
            spread = ask_prices[0] - bid_prices[0]
        
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
        
        day_high = day_stats['day_high'] or current_price
        day_low = day_stats['day_low'] or current_price
        day_open = day_stats['day_open'] or current_price
        day_range = day_high - day_low
        price_change_day = current_price - day_open
        price_change_pct = (price_change_day / day_open * 100) if day_open > 0 else 0
        
        # 9. Generate Trade Signal
        signals = []
        
        # Signal 1: CVD (3-min)
        if cvd_3m > 0:
            signals.append(1)  # Bullish
        elif cvd_3m < 0:
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
                "cvd_3min": round(float(cvd_3m), 2),
                "cvd_15min": round(float(cvd_15m), 2)
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
