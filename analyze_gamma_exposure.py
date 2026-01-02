#!/usr/bin/env python3
"""
Calculate Net Gamma Exposure (GEX) for NIFTY options
Tracks how much dealers are forced to hedge and in which direction
Helps identify trending vs mean-reversion market environments
"""

import os
import math
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
import pytz
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Database config
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '6432')),
    'database': os.getenv('DB_NAME', 'tradingdb'),
    'user': os.getenv('DB_USER', 'tradinguser'),
    'password': os.getenv('DB_PASSWORD')
}

IST = pytz.timezone('Asia/Kolkata')


def get_immediate_expiry(conn):
    """Get the nearest NIFTY option expiry"""
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
    SELECT DISTINCT expiry
    FROM instruments
    WHERE trading_symbol LIKE 'NIFTY%'
    AND instrument_type IN ('CE', 'PE')
    AND exchange = 'NFO'
    AND expiry >= CURRENT_DATE
    ORDER BY expiry
    LIMIT 1
    """
    
    try:
        cursor.execute(query)
        result = cursor.fetchone()
        return result['expiry'] if result else None
    finally:
        cursor.close()


def get_current_spot_price(conn):
    """Get the latest NIFTY spot price from ticks"""
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
    SELECT last_price
    FROM ticks
    WHERE instrument_token = (
        SELECT instrument_token FROM instruments 
        WHERE trading_symbol = 'NIFTY 50' LIMIT 1
    )
    ORDER BY time DESC
    LIMIT 1
    """
    
    try:
        cursor.execute(query)
        result = cursor.fetchone()
        return float(result['last_price']) if result else None
    finally:
        cursor.close()


def get_all_nifty_options_data(conn, expiry, cutoff_time=None):
    """Get latest tick data for all NIFTY options"""
    
    if cutoff_time is None:
        cutoff_time = datetime.now(IST)
    
    # Convert IST to UTC for database query
    cutoff_utc = cutoff_time.astimezone(pytz.UTC)
    
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
    WITH latest_ticks AS (
        SELECT 
            i.instrument_token,
            i.trading_symbol,
            i.strike,
            i.instrument_type,
            t.last_price,
            t.oi,
            t.volume_traded,
            ROW_NUMBER() OVER (PARTITION BY i.instrument_token ORDER BY t.time DESC) as rn
        FROM instruments i
        JOIN ticks t ON i.instrument_token = t.instrument_token
        WHERE i.segment = 'NFO-OPT'
        AND i.name LIKE 'NIFTY%'
        AND i.expiry = %s
        AND i.exchange = 'NFO'
        AND t.time <= %s
    )
    SELECT *
    FROM latest_ticks
    WHERE rn = 1
    ORDER BY strike, instrument_type
    """
    
    try:
        cursor.execute(query, (expiry, cutoff_utc))
        return cursor.fetchall()
    finally:
        cursor.close()


def calculate_gamma_approximation(distance_from_atm_points, time_to_expiry_days, iv=0.15):
    """
    Simplified gamma calculation using Gaussian approximation
    
    Gamma peaks at ATM and decays as we move away
    Increases as time to expiry decreases
    Increases with IV
    
    Args:
        distance_from_atm_points: How far from ATM (in points)
        time_to_expiry_days: Days until expiration
        iv: Implied volatility (0.15 = 15%)
    
    Returns:
        gamma_value: Normalized gamma value (0-1 scale)
    """
    
    # ATM gamma peak increases as expiry nears
    if time_to_expiry_days <= 0:
        time_factor = 1.0
    else:
        time_factor = 1.0 / math.sqrt(time_to_expiry_days)
    
    # IV impact: higher IV = wider distribution = lower ATM gamma
    iv_factor = 0.15 / iv if iv > 0 else 1.0
    
    # Distance decay: Gaussian curve
    # Normalize distance by ATM straddle width
    normalized_distance = distance_from_atm_points / (100 * iv_factor)
    
    # Gaussian bell curve centered at 0 (ATM)
    gamma_at_distance = math.exp(-0.5 * (normalized_distance ** 2))
    
    # Combine time and distance effects
    gamma = gamma_at_distance * time_factor * iv_factor
    
    return max(0.0, min(1.0, gamma))  # Clamp to 0-1


def calculate_net_gamma_exposure(options_data, spot_price, expiry_date):
    """
    Calculate Net Gamma Exposure across all strikes
    
    GEX = Sum of (OI × Gamma × Side)
    - Negative GEX = Dealers SHORT gamma (trending environment)
    - Positive GEX = Dealers LONG gamma (mean-reversion environment)
    """
    
    current_date = datetime.now(IST).date()
    time_to_expiry = (expiry_date - current_date).days
    
    total_gex = 0.0
    strike_gammas = {}
    
    for option in options_data:
        strike = float(option['strike'])
        oi = int(option['oi'])
        instrument_type = option['instrument_type']
        
        # Calculate distance from spot (in points)
        distance = abs(strike - spot_price)
        
        # Calculate gamma for this strike
        gamma = calculate_gamma_approximation(distance, time_to_expiry)
        
        # Store for detailed breakdown
        strike_gammas[f"{strike:.0f}{instrument_type}"] = {
            'gamma': gamma,
            'oi': oi,
            'distance': distance,
            'type': instrument_type
        }
        
        # GEX calculation
        # Calls: Dealers short calls = negative exposure (use -OI)
        # Puts: Dealers short puts = negative exposure (use -OI)
        # Both create negative GEX when ATM (dealers forced to hedge against moves)
        
        if instrument_type == 'CE':
            contribution = -oi * gamma  # Dealers typically short calls
        else:  # PE
            contribution = -oi * gamma  # Dealers typically short puts
        
        total_gex += contribution
    
    return total_gex, strike_gammas


def get_gex_interpretation(gex_value, abs_threshold=300):
    """Interpret GEX level and market environment"""
    
    if gex_value < -abs_threshold:
        environment = "SHORT GAMMA (Dealers Selling)"
        character = "Trending / Momentum"
        implications = [
            "✅ Breakouts follow through",
            "✅ Moves accelerate (dealer hedging adds fuel)",
            "✅ Trends extend further than expected",
            "❌ Reversals are violent",
            "❌ Whipsaws common in last 1-2 hours"
        ]
        
    elif gex_value > abs_threshold:
        environment = "LONG GAMMA (Dealers Buying)"
        character = "Mean Reversion / Choppy"
        implications = [
            "✅ Fakeouts common",
            "✅ Breaks fail and reverse",
            "✅ Range-bound action",
            "✅ Support/resistance hold better",
            "❌ Trending trades fail"
        ]
        
    else:
        environment = "NEUTRAL GAMMA"
        character = "Mixed / Balanced"
        implications = [
            "📊 Normal market conditions",
            "📊 Both trend and mean reversion possible",
            "📊 Watch for GEX shifts as indicator"
        ]
    
    return {
        'environment': environment,
        'character': character,
        'implications': implications
    }


def format_number(num):
    """Format large numbers in crore notation"""
    if abs(num) >= 10000000:
        return f"₹{num/10000000:.1f}Cr"
    return f"₹{num:.0f}"


def analyze_gamma_exposure(expiry=None, cutoff_time=None):
    """Main analysis function"""
    
    if cutoff_time is None:
        cutoff_time = datetime.now(IST)
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        # Get expiry
        if expiry is None:
            expiry = get_immediate_expiry(conn)
            if not expiry:
                print("❌ No NIFTY options found")
                return
        
        # Get current spot
        spot_price = get_current_spot_price(conn)
        if not spot_price:
            print("❌ Could not fetch spot price")
            return
        
        print(f"\n{'='*100}")
        print(f"🎲 NET GAMMA EXPOSURE ANALYSIS")
        print(f"{'='*100}")
        print(f"Spot Price: {spot_price:.2f}")
        print(f"Expiry Date: {expiry}")
        print(f"Analysis Time: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
        print(f"{'='*100}\n")
        
        # Get all options data
        options_data = get_all_nifty_options_data(conn, expiry, cutoff_time)
        
        if not options_data:
            print("❌ No options tick data found for this expiry")
            return
        
        print(f"📊 Processing {len(options_data)} option contracts...\n")
        
        # Calculate net gamma exposure
        gex, strike_gammas = calculate_net_gamma_exposure(options_data, spot_price, expiry)
        
        # Get interpretation
        interpretation = get_gex_interpretation(gex)
        
        # Display main result
        print(f"{'─'*100}")
        print(f"NET GAMMA EXPOSURE: {format_number(gex)}")
        print(f"{'─'*100}")
        print(f"Environment: {interpretation['environment']}")
        print(f"Market Character: {interpretation['character']}\n")
        
        print("Implications:")
        for implication in interpretation['implications']:
            print(f"  {implication}")
        
        # Detailed breakdown by strike
        print(f"\n{'─'*100}")
        print(f"GAMMA CONCENTRATION BY STRIKE")
        print(f"{'─'*100}\n")
        
        # Group by strike
        strikes_data = {}
        for symbol, gamma_info in strike_gammas.items():
            strike = symbol[:-2]  # Remove CE/PE suffix
            if strike not in strikes_data:
                strikes_data[strike] = {'calls': {}, 'puts': {}}
            
            if 'CE' in symbol:
                strikes_data[strike]['calls'] = gamma_info
            else:
                strikes_data[strike]['puts'] = gamma_info
        
        # Find highest gamma concentrations
        print(f"{'Strike':<10} {'Distance':<12} {'Call Gamma':<15} {'Put Gamma':<15} {'Combined OI':<15}")
        print(f"{'─'*100}")
        
        for strike_str in sorted(strikes_data.keys(), key=lambda x: float(x)):
            strike = float(strike_str)
            strike_info = strikes_data[strike_str]
            
            call_gamma = strike_info['calls'].get('gamma', 0)
            put_gamma = strike_info['puts'].get('gamma', 0)
            call_oi = strike_info['calls'].get('oi', 0)
            put_oi = strike_info['puts'].get('oi', 0)
            
            distance = abs(strike - spot_price)
            
            # Highlight ATM strikes (highest gamma)
            if distance < 50:
                marker = " ⭐ ATM"
            else:
                marker = ""
            
            print(f"{strike:>9.0f} {distance:>11.0f}pt " 
                  f"{call_gamma:>14.3f} {put_gamma:>14.3f} "
                  f"{(call_oi + put_oi):>14,.0f}{marker}")
        
        # Summary statistics
        print(f"\n{'─'*100}")
        print(f"SUMMARY STATISTICS")
        print(f"{'─'*100}\n")
        
        # Calculate some stats
        atm_range = [og for s, og in strike_gammas.items() if og['distance'] < 100]
        total_oi = sum(og['oi'] for og in strike_gammas.values())
        atm_oi = sum(og['oi'] for og in atm_range)
        
        print(f"Total Options OI: {total_oi:,} lots")
        print(f"ATM (±100pt) OI: {atm_oi:,} lots ({100*atm_oi/total_oi:.1f}%)")
        print(f"Gamma Concentration: {'High (tight range)' if atm_oi/total_oi > 0.6 else 'Dispersed'}")
        print(f"Likely Expiry Behavior: {'Pin to ATM strikes' if atm_oi/total_oi > 0.6 else 'Range-dependent'}")
        
        # Trading recommendations
        print(f"\n{'─'*100}")
        print(f"TRADING RECOMMENDATIONS")
        print(f"{'─'*100}\n")
        
        if gex < -300:
            print("🔥 SHORT GAMMA ENVIRONMENT")
            print("  ✅ DO: Ride trends, hold winners, use tight stops")
            print("  ❌ DON'T: Fade moves expecting quick reversal")
            print("  ⏰ CAUTION: Watch for dealer unwind in last 30 mins of expiry")
            
        elif gex > 300:
            print("🛡️  LONG GAMMA ENVIRONMENT")
            print("  ✅ DO: Sell breakouts, trade range-bound, scalp bounces")
            print("  ❌ DON'T: Chase momentum expecting follow-through")
            print("  ⏰ EDGE: Fades are high-probability")
            
        else:
            print("⚖️  NEUTRAL GAMMA ENVIRONMENT")
            print("  ✅ DO: Be flexible, watch for GEX shifts")
            print("  ✅ DO: Combine with OI positioning signals")
            print("  ⏰ WATCH: Market can swing either direction based on catalyst")
        
        print(f"\n{'='*100}\n")
        
        return {
            'gex': gex,
            'spot': spot_price,
            'expiry': expiry,
            'interpretation': interpretation,
            'timestamp': cutoff_time.isoformat()
        }
    
    finally:
        conn.close()


if __name__ == "__main__":
    analyze_gamma_exposure()
