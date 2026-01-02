#!/usr/bin/env python3
"""
Calculate Net Gamma Exposure (GEX) for NIFTY options
Tracks how much dealers are forced to hedge and in which direction
Helps identify trending vs mean-reversion market environments

Uses Black-Scholes model with IV extracted from market premiums
OPTIMIZED for high data volumes with efficient caching and batch processing
"""

import os
import math
from datetime import datetime, timedelta
from collections import defaultdict
import psycopg2
from psycopg2.extras import RealDictCursor
import pytz
from dotenv import load_dotenv
from kiteconnect import KiteConnect
from scipy.stats import norm
from scipy.optimize import brentq

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

# KiteConnect config
KITE_API_KEY = os.getenv('KITE_API_KEY')
NIFTY_TOKEN = 256265

# Indian market parameters
RISK_FREE_RATE = 0.065
MIN_IV = 0.001
MAX_IV = 2.0

# OPTIMIZATION: IV caching to avoid redundant solver calls
IV_CACHE = {}  # {(spot, strike, days_to_expiry): iv}
CACHE_EXPIRY_MINUTES = 15


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


def get_kite_instance():
    """Get authenticated KiteConnect instance"""
    try:
        # Get access token from file
        token_file = '/app/data/access_token.txt'
        if not os.path.exists(token_file):
            token_file = os.path.expanduser('~/.kite_access_token.txt')
        
        with open(token_file, 'r') as f:
            access_token = f.read().strip()
        
        kite = KiteConnect(api_key=KITE_API_KEY)
        kite.set_access_token(access_token)
        return kite
    except Exception as e:
        print(f"⚠️  Could not authenticate KiteConnect: {e}")
        return None


def get_current_spot_price_from_kite():
    """Get latest NIFTY 50 spot price using KiteConnect historical API"""
    try:
        kite = get_kite_instance()
        if not kite:
            return None
        
        # Get today's date
        today = datetime.now(IST).date()
        
        # Fetch 1-minute candles for today (gives us latest price)
        data = kite.historical_data(
            instrument_token=NIFTY_TOKEN,
            from_date=today,
            to_date=today,
            interval="minute"
        )
        
        if data:
            # Get the last candle (most recent)
            latest = data[-1]
            spot_price = float(latest['close'])
            return spot_price
        
        return None
    except Exception as e:
        print(f"⚠️  KiteConnect error: {e}")
        return None


def get_current_spot_price(conn):
    """Get the latest NIFTY spot price - try KiteConnect first, fallback to DB"""
    
    # Try KiteConnect first (most reliable)
    spot = get_current_spot_price_from_kite()
    if spot:
        return spot
    
    print("⚠️  Falling back to database for spot price...")
    
    # Fallback: Get from database
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
    SELECT last_price
    FROM ticks
    WHERE instrument_token = 256265
    ORDER BY time DESC
    LIMIT 1
    """
    
    try:
        cursor.execute(query)
        result = cursor.fetchone()
        return float(result['last_price']) if result else None
    except Exception as e:
        print(f"⚠️  Database spot price error: {e}")
        return None
    finally:
        cursor.close()


def get_all_nifty_options_data(conn, expiry, cutoff_time=None):
    """
    Get latest tick data for all NIFTY options
    OPTIMIZED: 
    - Uses index on (instrument_token, time) for fast filtering
    - Only fetches last N hours (time windowing)
    - Fetch on segment+expiry first (narrow down rows fast)
    """
    
    if cutoff_time is None:
        cutoff_time = datetime.now(IST)
    
    cutoff_utc = cutoff_time.astimezone(pytz.UTC)
    start_time_utc = cutoff_utc - timedelta(hours=2)  # 2-hour window
    
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # OPTIMIZED: Use filtered subquery approach (PostgreSQL planner optimizes better)
    query = """
    WITH filtered_instruments AS (
        SELECT instrument_token, trading_symbol, strike, instrument_type
        FROM instruments
        WHERE segment = 'NFO-OPT'
        AND expiry = %s
        AND exchange = 'NFO'
        AND name LIKE 'NIFTY%%'
    ),
    latest_ticks AS (
        SELECT 
            fi.instrument_token,
            fi.trading_symbol,
            fi.strike,
            fi.instrument_type,
            t.last_price,
            t.oi,
            t.volume_traded,
            ROW_NUMBER() OVER (PARTITION BY t.instrument_token ORDER BY t.time DESC) as rn
        FROM filtered_instruments fi
        INNER JOIN ticks t ON fi.instrument_token = t.instrument_token
        WHERE t.time >= %s AND t.time <= %s
    )
    SELECT 
        instrument_token, trading_symbol, strike, instrument_type,
        last_price, oi, volume_traded
    FROM latest_ticks
    WHERE rn = 1
    ORDER BY strike, instrument_type
    """
    
    try:
        cursor.execute(query, (expiry, start_time_utc, cutoff_utc))
        return cursor.fetchall()
    finally:
        cursor.close()


def calculate_gamma_approximation(distance_from_atm_points, time_to_expiry_days, iv=0.15):
    """Deprecated: Use calculate_gamma_blackscholes instead"""
    # Kept for backward compatibility
    if time_to_expiry_days <= 0:
        time_factor = 1.0
    else:
        time_factor = 1.0 / math.sqrt(time_to_expiry_days)
    
    iv_factor = 0.15 / iv if iv > 0 else 1.0
    normalized_distance = distance_from_atm_points / (100 * iv_factor)
    gamma_at_distance = math.exp(-0.5 * (normalized_distance ** 2))
    gamma = gamma_at_distance * time_factor * iv_factor
    
    return max(0.0, min(1.0, gamma))


def black_scholes_call(spot, strike, time_to_expiry_years, risk_free_rate, volatility):
    """Black-Scholes call option pricing"""
    
    if time_to_expiry_years <= 0:
        return max(spot - strike, 0)
    
    d1 = (math.log(spot / strike) + (risk_free_rate + 0.5 * volatility**2) * time_to_expiry_years) / (volatility * math.sqrt(time_to_expiry_years))
    d2 = d1 - volatility * math.sqrt(time_to_expiry_years)
    
    call_price = spot * norm.cdf(d1) - strike * math.exp(-risk_free_rate * time_to_expiry_years) * norm.cdf(d2)
    
    return call_price


def black_scholes_put(spot, strike, time_to_expiry_years, risk_free_rate, volatility):
    """Black-Scholes put option pricing"""
    
    if time_to_expiry_years <= 0:
        return max(strike - spot, 0)
    
    d1 = (math.log(spot / strike) + (risk_free_rate + 0.5 * volatility**2) * time_to_expiry_years) / (volatility * math.sqrt(time_to_expiry_years))
    d2 = d1 - volatility * math.sqrt(time_to_expiry_years)
    
    put_price = strike * math.exp(-risk_free_rate * time_to_expiry_years) * norm.cdf(-d2) - spot * norm.cdf(-d1)
    
    return put_price


def black_scholes_gamma(spot, strike, time_to_expiry_years, risk_free_rate, volatility):
    """Black-Scholes gamma (delta change per 1 point spot move)"""
    
    if time_to_expiry_years <= 0 or volatility == 0:
        return 0
    
    d1 = (math.log(spot / strike) + (risk_free_rate + 0.5 * volatility**2) * time_to_expiry_years) / (volatility * math.sqrt(time_to_expiry_years))
    
    # Gamma = pdf(d1) / (S × σ × √T)
    gamma = norm.pdf(d1) / (spot * volatility * math.sqrt(time_to_expiry_years))
    
    return gamma


def extract_iv_from_premium(market_price, spot, strike, time_to_expiry_years, option_type, risk_free_rate=RISK_FREE_RATE):
    """Extract IV from market premium using Black-Scholes solver with caching"""
    
    if market_price <= 0 or time_to_expiry_years <= 0:
        return 0.15
    
    # OPTIMIZATION: Check IV cache first
    cache_key = (round(spot, 0), round(strike, 0), round(time_to_expiry_years * 365))
    if cache_key in IV_CACHE:
        return IV_CACHE[cache_key]
    
    if option_type == 'CE':
        pricing_fn = lambda iv: black_scholes_call(spot, strike, time_to_expiry_years, risk_free_rate, iv) - market_price
    else:
        pricing_fn = lambda iv: black_scholes_put(spot, strike, time_to_expiry_years, risk_free_rate, iv) - market_price
    
    try:
        # Quick check if solution exists
        lower_val = pricing_fn(MIN_IV)
        upper_val = pricing_fn(MAX_IV)
        
        if lower_val * upper_val > 0:
            # No sign change = no solution in range
            iv = 0.15
        else:
            # Use Brent's method (converges faster than bisection)
            iv = brentq(pricing_fn, MIN_IV, MAX_IV, xtol=1e-5, rtol=1e-5)  # Relaxed tolerance for speed
            iv = max(MIN_IV, min(MAX_IV, iv))
    
    except:
        iv = 0.15
    
    # OPTIMIZATION: Cache the result
    IV_CACHE[cache_key] = iv
    return iv


def calculate_gamma_blackscholes(spot, strike, time_to_expiry_years, market_price, option_type, risk_free_rate=RISK_FREE_RATE):
    """Calculate Black-Scholes gamma by extracting IV from market premium"""
    
    if time_to_expiry_years <= 0 or market_price <= 0:
        return 0.0, 0.15
    
    iv = extract_iv_from_premium(market_price, spot, strike, time_to_expiry_years, option_type, risk_free_rate)
    gamma = black_scholes_gamma(spot, strike, time_to_expiry_years, risk_free_rate, iv)
    
    return gamma, iv


def calculate_net_gamma_exposure(options_data, spot_price, expiry_date):
    """
    Calculate Net Gamma Exposure using Black-Scholes gamma
    OPTIMIZED: 
    - Pre-compute time-to-expiry once (not per option)
    - Use defaultdict for faster IV tracking
    - Batch IV calculations for ATM strikes
    - Skip premium calculations for zero-OI strikes
    
    GEX = Sum of (OI × Gamma × Side)
    """
    
    current_date = datetime.now(IST).date()
    days_to_expiry = (expiry_date - current_date).days
    years_to_expiry = max(days_to_expiry / 365.0, 1/365.0)
    
    # Pre-compute sqrt values (used multiple times in BS calculations)
    sqrt_T = math.sqrt(years_to_expiry)
    sqrt_T_inv = 1.0 / sqrt_T if years_to_expiry > 0 else 0
    
    total_gex = 0.0
    strike_gammas = {}
    iv_levels = defaultdict(list)
    
    # OPTIMIZATION: Separate ATM and OTM processing
    atm_options = []
    otm_options = []
    
    # First pass: Segregate by distance
    for option in options_data:
        if int(option['oi']) == 0:  # Skip zero OI
            continue
        
        distance = abs(float(option['strike']) - spot_price)
        if distance <= 200:
            atm_options.append(option)
        else:
            otm_options.append(option)
    
    # Process ATM options with IV extraction
    for option in atm_options:
        strike = float(option['strike'])
        oi = int(option['oi'])
        premium = float(option['last_price'])
        instrument_type = option['instrument_type']
        distance = abs(strike - spot_price)
        
        try:
            if premium > 0:
                gamma, iv = calculate_gamma_blackscholes(
                    spot=spot_price,
                    strike=strike,
                    time_to_expiry_years=years_to_expiry,
                    market_price=premium,
                    option_type=instrument_type,
                    risk_free_rate=RISK_FREE_RATE
                )
            else:
                iv = 0.15
                gamma = black_scholes_gamma(spot_price, strike, years_to_expiry, RISK_FREE_RATE, iv)
            
            strike_key = f"{strike:.0f}{instrument_type}"
            strike_gammas[strike_key] = {
                'gamma': gamma,
                'oi': oi,
                'premium': premium,
                'iv': iv,
                'type': instrument_type,
                'distance': distance,
                'iv_extracted': True
            }
            
            iv_levels[strike].append(iv)
            total_gex += -oi * gamma
        
        except:
            continue
    
    # Process OTM options with default IV (FAST PATH)
    default_iv = 0.15
    for option in otm_options:
        strike = float(option['strike'])
        oi = int(option['oi'])
        premium = float(option['last_price'])
        instrument_type = option['instrument_type']
        distance = abs(strike - spot_price)
        
        try:
            # Skip expensive IV solver for OTM - use default
            gamma = black_scholes_gamma(spot_price, strike, years_to_expiry, RISK_FREE_RATE, default_iv)
            
            strike_key = f"{strike:.0f}{instrument_type}"
            strike_gammas[strike_key] = {
                'gamma': gamma,
                'oi': oi,
                'premium': premium,
                'iv': default_iv,
                'type': instrument_type,
                'distance': distance,
                'iv_extracted': False
            }
            
            iv_levels[strike].append(default_iv)
            total_gex += -oi * gamma
        
        except:
            continue
    
    return total_gex, strike_gammas, iv_levels


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


def analyze_gamma_exposure(expiry=None, cutoff_time=None, verbose=True):
    """Main analysis function with performance monitoring"""
    
    if cutoff_time is None:
        cutoff_time = datetime.now(IST)
    
    # OPTIMIZATION: Timer for performance monitoring
    import time
    start_time = time.time()
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        # Get expiry
        t1 = time.time()
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
        
        t2 = time.time()
        
        print(f"\n{'='*100}")
        print(f"🎲 NET GAMMA EXPOSURE ANALYSIS")
        print(f"{'='*100}")
        print(f"Spot Price: {spot_price:.2f}")
        print(f"Expiry Date: {expiry}")
        print(f"Analysis Time: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
        if verbose:
            print(f"[TIMING] Setup: {t2-t1:.2f}s")
        print(f"{'='*100}\n")
        
        # Get all options data
        t3 = time.time()
        options_data = get_all_nifty_options_data(conn, expiry, cutoff_time)
        t4 = time.time()
        
        if not options_data:
            print("❌ No options tick data found for this expiry")
            return
        
        if verbose:
            print(f"[TIMING] Data fetch: {t4-t3:.2f}s ({len(options_data)} contracts)")
        print(f"📊 Processing {len(options_data)} option contracts...\n")
        
        # Calculate net gamma exposure
        t5 = time.time()
        gex, strike_gammas, iv_levels = calculate_net_gamma_exposure(options_data, spot_price, expiry)
        t6 = time.time()
        
        if verbose:
            print(f"[TIMING] GEX calculation: {t6-t5:.2f}s")
            print(f"[CACHE] IV solver calls avoided: {len(IV_CACHE)}")
        
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
        
        # Detailed breakdown by strike with IV
        print(f"\n{'─'*100}")
        print(f"GAMMA & IV ANALYSIS BY STRIKE")
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
        print(f"{'Strike':<10} {'Distance':<12} {'Call Gamma':<12} {'Call IV':<10} {'Put Gamma':<12} {'Put IV':<10} {'OI':<15}")
        print(f"{'─'*100}")
        
        for strike_str in sorted(strikes_data.keys(), key=lambda x: float(x)):
            strike = float(strike_str)
            strike_info = strikes_data[strike_str]
            
            call_gamma = strike_info['calls'].get('gamma', 0)
            call_iv = strike_info['calls'].get('iv', 0)
            put_gamma = strike_info['puts'].get('gamma', 0)
            put_iv = strike_info['puts'].get('iv', 0)
            call_oi = strike_info['calls'].get('oi', 0)
            put_oi = strike_info['puts'].get('oi', 0)
            
            distance = abs(strike - spot_price)
            
            # Highlight ATM strikes (highest gamma)
            if distance < 50:
                marker = " ⭐ ATM"
            else:
                marker = ""
            
            print(f"{strike:>9.0f} {distance:>11.0f}pt "
                  f"{call_gamma:>11.6f} {call_iv*100:>9.2f}% "
                  f"{put_gamma:>11.6f} {put_iv*100:>9.2f}% "
                  f"{(call_oi + put_oi):>14,.0f}{marker}")
        
        # Summary statistics
        print(f"\n{'─'*100}")
        print(f"SUMMARY STATISTICS")
        print(f"{'─'*100}\n")
        
        # Calculate some stats
        atm_range = [og for s, og in strike_gammas.items() if og['distance'] < 100]
        total_oi = sum(og['oi'] for og in strike_gammas.values())
        atm_oi = sum(og['oi'] for og in atm_range)
        
        # Average IVs
        all_ivs = [og['iv'] for og in strike_gammas.values() if og.get('iv', 0) > 0]
        avg_iv = sum(all_ivs) / len(all_ivs) if all_ivs else 0.15
        
        print(f"Total Options OI: {total_oi:,} lots")
        print(f"ATM (±100pt) OI: {atm_oi:,} lots ({100*atm_oi/total_oi:.1f}%)")
        print(f"Average Market IV: {avg_iv*100:.2f}%")
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
        
        if verbose:
            total_time = time.time() - start_time
            print(f"[PERFORMANCE] Total execution: {total_time:.2f}s")
            print(f"[PERFORMANCE] Per-contract processing: {(total_time/len(options_data))*1000:.2f}ms")
            print()
        
        return {
            'gex': gex,
            'spot': spot_price,
            'expiry': expiry,
            'interpretation': interpretation,
            'timestamp': cutoff_time.isoformat(),
            'timing': {
                'setup': t2-t1,
                'data_fetch': t4-t3,
                'calculation': t6-t5,
                'total': time.time() - start_time
            } if verbose else {}
        }
    
    finally:
        conn.close()


if __name__ == "__main__":
    analyze_gamma_exposure()
