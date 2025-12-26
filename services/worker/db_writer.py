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
        
        # Build INSERT with ON CONFLICT DO UPDATE
        cols_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Columns to update on conflict (all except primary key)
        update_cols = [col for col in columns if col not in ['time', 'instrument_token']]
        update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
        
        insert_sql = f"""
            INSERT INTO ticks ({cols_str})
            VALUES ({placeholders})
            ON CONFLICT (time, instrument_token) 
            DO UPDATE SET {update_set}
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
        
        # Execute batch insert with ON CONFLICT
        execute_batch(cursor, insert_sql, data_tuples, page_size=500)
        
        # Commit transaction
        conn.commit()
        
        rows_inserted = len(ticks_to_insert)
        
        logger.info(
            "bulk_insert_successful",
            rows_affected=rows_inserted,
            original_batch_size=original_count,
            deduped_batch_size=deduped_count
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
    Fallback bulk insert using SQLAlchemy bulk_insert_mappings
    Slower than COPY but more compatible
    
    Args:
        ticks: List of tick dictionaries
    
    Returns:
        int: Number of rows inserted
    """
    session = SessionLocal()
    
    try:
        # Use bulk_insert_mappings for better performance than individual inserts
        session.bulk_insert_mappings(
            Tick,
            ticks,
            return_defaults=False
        )
        
        session.commit()
        
        logger.info(
            "fallback_insert_successful",
            rows_inserted=len(ticks)
        )
        
        return len(ticks)
    
    except Exception as e:
        session.rollback()
        logger.error("fallback_insert_failed", error=str(e))
        raise
    
    finally:
        session.close()


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
