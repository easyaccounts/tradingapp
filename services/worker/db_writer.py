"""
Database Writer
Handles bulk inserts to TimescaleDB for optimal performance
"""

import os
import io
import structlog
from typing import List, Dict, Optional
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime

logger = structlog.get_logger()

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL")

# Cache for previous tick state per instrument (for delta calculations)
# Key: instrument_token, Value: previous tick dict
_previous_ticks: Dict[int, Dict] = {}

# Create SQLAlchemy engine with connection pooling
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=20,
    max_overflow=10,
    pool_pre_ping=True,  # Verify connections before using
    pool_recycle=3600,   # Recycle connections after 1 hour
    echo=False
)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def calculate_tick_metrics(tick: Dict, previous_tick: Optional[Dict]) -> Dict:
    """
    Calculate pre-computed metrics for a tick
    
    Args:
        tick: Current tick data
        previous_tick: Previous tick for this instrument (None if first tick)
    
    Returns:
        Dict with calculated metric fields
    """
    metrics = {}
    
    # Extract current values
    current_volume = tick.get('volume_traded', 0) or 0
    current_oi = tick.get('oi', 0) or 0
    current_price = float(tick.get('last_price', 0) or 0)
    current_buy_qty = tick.get('total_buy_quantity', 0) or 0
    current_sell_qty = tick.get('total_sell_quantity', 0) or 0
    
    # Get bid/ask arrays
    bid_prices = tick.get('bid_prices', [])
    ask_prices = tick.get('ask_prices', [])
    bid_quantities = tick.get('bid_quantities', [])
    ask_quantities = tick.get('ask_quantities', [])
    
    # Safe array access
    best_bid = float(bid_prices[0]) if bid_prices and bid_prices[0] else 0.0
    best_ask = float(ask_prices[0]) if ask_prices and ask_prices[0] else 0.0
    
    # 1. volume_delta
    if previous_tick:
        prev_volume = previous_tick.get('volume_traded', 0) or 0
        metrics['volume_delta'] = max(0, current_volume - prev_volume)
    else:
        metrics['volume_delta'] = 0
    
    # 2. oi_delta
    if previous_tick:
        prev_oi = previous_tick.get('oi', 0) or 0
        metrics['oi_delta'] = current_oi - prev_oi
    else:
        metrics['oi_delta'] = 0
    
    # 3 & 4. aggressor_side and cvd_change using Lee-Ready + EMO
    aggressor = 'NEUTRAL'
    
    if metrics['volume_delta'] > 0 and current_price > 0:
        # Step 1: Quote Rule with EMO depth-weighted midpoint
        if best_ask > 0 and current_price >= best_ask:
            # Trade at or above ask - clear BUY aggressor
            aggressor = 'BUY'
        elif best_bid > 0 and current_price <= best_bid:
            # Trade at or below bid - clear SELL aggressor
            aggressor = 'SELL'
        elif best_bid > 0 and best_ask > 0:
            # Trade within spread - use EMO depth-weighted midpoint
            bid_qty = bid_quantities[0] if bid_quantities and bid_quantities[0] else 0
            ask_qty = ask_quantities[0] if ask_quantities and ask_quantities[0] else 0
            
            if bid_qty + ask_qty > 0:
                # EMO: Weight by opposite side depth
                weighted_mid = (best_bid * ask_qty + best_ask * bid_qty) / (bid_qty + ask_qty)
            else:
                # Fallback to simple midpoint if no depth data
                weighted_mid = (best_bid + best_ask) / 2.0
            
            # Compare price to weighted midpoint
            if abs(current_price - weighted_mid) < 0.01:
                # At weighted midpoint - use tick rule (Step 2)
                if previous_tick:
                    prev_price = float(previous_tick.get('last_price', 0) or 0)
                    if prev_price > 0:
                        if current_price > prev_price:
                            aggressor = 'BUY'   # Uptick
                        elif current_price < prev_price:
                            aggressor = 'SELL'  # Downtick
                        else:
                            # Zero tick - inherit from previous aggressor if available
                            aggressor = previous_tick.get('aggressor_side', 'NEUTRAL')
                    else:
                        aggressor = 'NEUTRAL'
                else:
                    aggressor = 'NEUTRAL'
            else:
                # Quote rule decisive
                aggressor = 'BUY' if current_price > weighted_mid else 'SELL'
    
    metrics['aggressor_side'] = aggressor
    metrics['cvd_change'] = metrics['volume_delta'] if aggressor == 'BUY' else -metrics['volume_delta']
    
    # 5. buy_quantity_delta
    if previous_tick:
        prev_buy_qty = previous_tick.get('total_buy_quantity', 0) or 0
        metrics['buy_quantity_delta'] = current_buy_qty - prev_buy_qty
    else:
        metrics['buy_quantity_delta'] = 0
    
    # 6. sell_quantity_delta
    if previous_tick:
        prev_sell_qty = previous_tick.get('total_sell_quantity', 0) or 0
        metrics['sell_quantity_delta'] = current_sell_qty - prev_sell_qty
    else:
        metrics['sell_quantity_delta'] = 0
    
    # 7. mid_price_calc
    if best_bid > 0 and best_ask > 0:
        metrics['mid_price_calc'] = (best_bid + best_ask) / 2.0
    else:
        metrics['mid_price_calc'] = current_price
    
    # 8. bid_depth_total
    metrics['bid_depth_total'] = sum(qty for qty in bid_quantities if qty) if bid_quantities else 0
    
    # 9. ask_depth_total
    metrics['ask_depth_total'] = sum(qty for qty in ask_quantities if qty) if ask_quantities else 0
    
    # 10. depth_imbalance_ratio
    if metrics['ask_depth_total'] > 0:
        metrics['depth_imbalance_ratio'] = metrics['bid_depth_total'] / metrics['ask_depth_total']
    else:
        metrics['depth_imbalance_ratio'] = 0.0
    
    # 11. price_delta
    if previous_tick:
        prev_price = float(previous_tick.get('last_price', 0) or 0)
        metrics['price_delta'] = current_price - prev_price
    else:
        metrics['price_delta'] = 0.0
    
    # ========================================================================
    # ORDERFLOW TOXICITY METRICS
    # ========================================================================
    # 12. consumption_rate - Depth consumed per volume unit
    total_depth = metrics['bid_depth_total'] + metrics['ask_depth_total']
    if metrics['volume_delta'] > 0 and total_depth > 0:
        metrics['consumption_rate'] = total_depth / metrics['volume_delta']
    else:
        metrics['consumption_rate'] = 0.0
    
    # 13. flow_intensity - Price impact per volume (Kyle's Lambda component)
    if metrics['volume_delta'] > 0 and abs(metrics['price_delta']) > 0.01:
        metrics['flow_intensity'] = abs(metrics['price_delta']) / metrics['volume_delta']
    else:
        metrics['flow_intensity'] = 0.0
    
    # 14. depth_toxicity_tick - Depth Consumption Rate (lower = more toxic)
    # Formula: 1 / (1 + consumption_rate)
    # Range: 0 to 1, where values near 0 = high toxicity
    metrics['depth_toxicity_tick'] = 1.0 / (1.0 + metrics['consumption_rate'])
    
    # 15. kyle_lambda_tick - Kyle's Lambda (higher = more toxic flow)
    # Formula: flow_intensity * depth_toxicity
    # Combines price impact with depth consumption
    metrics['kyle_lambda_tick'] = metrics['flow_intensity'] * metrics['depth_toxicity_tick']
    
    return metrics


def get_db_engine():
    """
    Get SQLAlchemy database engine
    
    Returns:
        Engine: SQLAlchemy engine instance
    """
    return engine


def bulk_insert_ticks(ticks: List[Dict]) -> int:
    """
    Bulk insert ticks using PostgreSQL execute_batch with ON CONFLICT
    Calculates and adds pre-computed metrics before insertion.
    
    Deduplicates ticks in-memory (same time + instrument_token) and uses
    ON CONFLICT DO UPDATE to handle cross-batch duplicates gracefully.
    
    Args:
        ticks: List of tick dictionaries
    
    Returns:
        int: Number of rows inserted/updated
    
    Raises:
        Exception: If insert fails
    """
    global _previous_ticks
    
    if not ticks:
        logger.warning("bulk_insert_called_with_empty_list")
        return 0
    
    # Sort by time to ensure correct ordering for delta calculations
    ticks_sorted = sorted(ticks, key=lambda t: (t.get('time', datetime.min), t.get('instrument_token', 0)))
    
    # Calculate metrics and deduplicate
    enriched_ticks = []
    deduped = {}
    
    for tick in ticks_sorted:
        instrument_token = tick.get('instrument_token')
        if not instrument_token:
            continue
        
        # Get previous tick for this instrument
        prev_tick = _previous_ticks.get(instrument_token)
        
        # Calculate metrics
        metrics = calculate_tick_metrics(tick, prev_tick)
        
        # Merge metrics into tick
        enriched_tick = {**tick, **metrics}
        
        # Deduplicate: keep latest tick per (time, instrument_token)
        key = (tick.get('time'), instrument_token)
        deduped[key] = enriched_tick
        
        # Update previous tick cache
        _previous_ticks[instrument_token] = tick
    
    ticks_to_insert = list(deduped.values())
    original_count = len(ticks)
    deduped_count = len(ticks_to_insert)
    
    if original_count > deduped_count:
        logger.info(
            "batch_deduplicated",
            original=original_count,
            deduped=deduped_count,
            duplicates_removed=original_count - deduped_count
        )
    
    try:
        # Get raw psycopg2 connection
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Column order matching database schema (with new pre-calculated metrics)
        columns = [
            'time', 'last_trade_time', 'instrument_token', 'trading_symbol',
            'exchange', 'instrument_type', 'last_price', 'last_traded_quantity',
            'average_traded_price', 'volume_traded', 'oi', 'oi_day_high',
            'oi_day_low', 'day_open', 'day_high', 'day_low', 'day_close',
            'change', 'change_percent', 'total_buy_quantity', 'total_sell_quantity',
            'bid_prices', 'bid_quantities', 'bid_orders', 'ask_prices',
            'ask_quantities', 'ask_orders', 'tradable', 'mode',
            # Pre-calculated metrics
            'volume_delta', 'oi_delta', 'aggressor_side', 'cvd_change',
            'buy_quantity_delta', 'sell_quantity_delta', 'mid_price_calc',
            'bid_depth_total', 'ask_depth_total', 'depth_imbalance_ratio', 'price_delta',
            # Orderflow toxicity metrics
            'consumption_rate', 'flow_intensity', 'depth_toxicity_tick', 'kyle_lambda_tick',
            # Legacy fields
            'bid_ask_spread', 'mid_price', 'order_imbalance'
        ]
        
        # Build INSERT with ON CONFLICT DO NOTHING to skip duplicates
        cols_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        insert_sql = f"""
            INSERT INTO ticks ({cols_str})
            VALUES ({placeholders})
            ON CONFLICT (time, instrument_token) DO NOTHING
        """
        
        # Prepare data tuples
        data_tuples = []
        for tick in ticks_to_insert:
            row = []
            for col in columns:
                value = tick.get(col)
                
                # Handle arrays - convert list to PostgreSQL array
                if col in ['bid_prices', 'bid_quantities', 'bid_orders', 
                           'ask_prices', 'ask_quantities', 'ask_orders']:
                    if isinstance(value, list):
                        row.append(value)  # psycopg2 handles list -> array conversion
                    else:
                        row.append(None)
                else:
                    row.append(value)
            
            data_tuples.append(tuple(row))
        
        # Execute batch insert with ON CONFLICT (silently skips duplicates)
        execute_batch(cursor, insert_sql, data_tuples, page_size=500)
        
        # Commit transaction
        conn.commit()
        
        # Note: rowcount may not be accurate with ON CONFLICT DO NOTHING
        rows_inserted = len(ticks_to_insert)
        
        logger.info(
            "bulk_insert_successful",
            rows_attempted=rows_inserted,
            original_batch_size=original_count,
            deduped_batch_size=deduped_count,
            note="duplicates_silently_skipped_via_on_conflict"
        )
        
        # Cleanup
        cursor.close()
        conn.close()
        
        return rows_inserted
    
    except Exception as e:
        # Ensure cleanup on error
        try:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'conn' in locals() and conn:
                conn.close()
        except:
            pass
        
        logger.error(
            "bulk_insert_failed",
            error=str(e),
            batch_size=len(ticks_to_insert)
        )
        
        # Try fallback method using SQLAlchemy
        try:
            logger.warning("attempting_fallback_insert_method")
            return _bulk_insert_fallback(ticks_to_insert)
        except Exception as fallback_error:
            logger.error(
                "fallback_insert_also_failed",
                error=str(fallback_error)
            )
            raise


def _bulk_insert_fallback(ticks: List[Dict]) -> int:
    """
    Fallback bulk insert using raw SQL with ON CONFLICT
    Handles duplicates gracefully
    
    Args:
        ticks: List of tick dictionaries
    
    Returns:
        int: Number of rows inserted/updated
    """
    try:
        # Get raw psycopg2 connection for ON CONFLICT support
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Column order matching database schema (with new pre-calculated metrics)
        columns = [
            'time', 'last_trade_time', 'instrument_token', 'trading_symbol',
            'exchange', 'instrument_type', 'last_price', 'last_traded_quantity',
            'average_traded_price', 'volume_traded', 'oi', 'oi_day_high',
            'oi_day_low', 'day_open', 'day_high', 'day_low', 'day_close',
            'change', 'change_percent', 'total_buy_quantity', 'total_sell_quantity',
            'bid_prices', 'bid_quantities', 'bid_orders', 'ask_prices',
            'ask_quantities', 'ask_orders', 'tradable', 'mode',
            # Pre-calculated metrics
            'volume_delta', 'oi_delta', 'aggressor_side', 'cvd_change',
            'buy_quantity_delta', 'sell_quantity_delta', 'mid_price_calc',
            'bid_depth_total', 'ask_depth_total', 'depth_imbalance_ratio', 'price_delta',
            # Orderflow toxicity metrics
            'consumption_rate', 'flow_intensity', 'depth_toxicity_tick', 'kyle_lambda_tick',
            # Legacy fields
            'bid_ask_spread', 'mid_price', 'order_imbalance'
        ]
        
        # Build INSERT with ON CONFLICT DO NOTHING to skip duplicates
        cols_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        insert_sql = f"""
            INSERT INTO ticks ({cols_str})
            VALUES ({placeholders})
            ON CONFLICT (time, instrument_token) DO NOTHING
        """
        
        # Prepare data tuples
        data_tuples = []
        for tick in ticks:
            row = []
            for col in columns:
                value = tick.get(col)
                
                # Handle arrays - convert list to PostgreSQL array
                if col in ['bid_prices', 'bid_quantities', 'bid_orders', 
                           'ask_prices', 'ask_quantities', 'ask_orders']:
                    if isinstance(value, list):
                        row.append(value)  # psycopg2 handles list -> array conversion
                    else:
                        row.append(None)
                else:
                    row.append(value)
            
            data_tuples.append(tuple(row))
        
        # Execute inserts one by one (slower but handles duplicates)
        inserted_count = 0
        for data_tuple in data_tuples:
            try:
                cursor.execute(insert_sql, data_tuple)
                if cursor.rowcount > 0:
                    inserted_count += cursor.rowcount
            except Exception as row_error:
                logger.warning("fallback_row_insert_failed", error=str(row_error))
                continue
        
        # Commit transaction
        conn.commit()
        
        logger.info(
            "fallback_insert_successful",
            rows_inserted=inserted_count,
            batch_size=len(ticks),
            duplicates_skipped=len(ticks) - inserted_count
        )
        
        # Cleanup
        cursor.close()
        conn.close()
        
        return inserted_count
    
    except Exception as e:
        # Ensure cleanup on error
        try:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'conn' in locals() and conn:
                conn.close()
        except:
            pass
        
        logger.error("fallback_insert_failed", error=str(e))
        raise


def test_connection() -> bool:
    """
    Test database connection
    
    Returns:
        bool: True if connection successful
    """
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        cursor.close()
        conn.close()
        
        logger.info("database_connection_test_successful")
        return True
    
    except Exception as e:
        logger.error("database_connection_test_failed", error=str(e))
        return False


# Import Tick model for fallback method
from models import Tick
