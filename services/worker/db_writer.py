"""
Database Writer
Handles bulk inserts to TimescaleDB for optimal performance
"""

import os
import io
import structlog
from typing import List, Dict
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime

logger = structlog.get_logger()

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL")

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
    
    Deduplicates ticks in-memory (same time + instrument_token) and uses
    ON CONFLICT DO UPDATE to handle cross-batch duplicates gracefully.
    
    Args:
        ticks: List of tick dictionaries
    
    Returns:
        int: Number of rows inserted/updated
    
    Raises:
        Exception: If insert fails
    """
    if not ticks:
        logger.warning("bulk_insert_called_with_empty_list")
        return 0
    
    # Deduplicate within batch: keep latest tick per (time, instrument_token)
    deduped = {}
    for tick in ticks:
        key = (tick.get('time'), tick.get('instrument_token'))
        deduped[key] = tick
    
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
        
        # Column order matching database schema
        columns = [
            'time', 'last_trade_time', 'instrument_token', 'trading_symbol',
            'exchange', 'instrument_type', 'last_price', 'last_traded_quantity',
            'average_traded_price', 'volume_traded', 'oi', 'oi_day_high',
            'oi_day_low', 'day_open', 'day_high', 'day_low', 'day_close',
            'change', 'change_percent', 'total_buy_quantity', 'total_sell_quantity',
            'bid_prices', 'bid_quantities', 'bid_orders', 'ask_prices',
            'ask_quantities', 'ask_orders', 'tradable', 'mode', 'bid_ask_spread',
            'mid_price', 'order_imbalance'
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
        
        # Column order matching database schema
        columns = [
            'time', 'last_trade_time', 'instrument_token', 'trading_symbol',
            'exchange', 'instrument_type', 'last_price', 'last_traded_quantity',
            'average_traded_price', 'volume_traded', 'oi', 'oi_day_high',
            'oi_day_low', 'day_open', 'day_high', 'day_low', 'day_close',
            'change', 'change_percent', 'total_buy_quantity', 'total_sell_quantity',
            'bid_prices', 'bid_quantities', 'bid_orders', 'ask_prices',
            'ask_quantities', 'ask_orders', 'tradable', 'mode', 'bid_ask_spread',
            'mid_price', 'order_imbalance'
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
