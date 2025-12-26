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
    Bulk insert ticks using PostgreSQL COPY for maximum performance
    
    This method uses PostgreSQL's COPY command which is the fastest way
    to insert large batches of data.
    
    Args:
        ticks: List of tick dictionaries
    
    Returns:
        int: Number of rows inserted
    
    Raises:
        Exception: If insert fails
    """
    if not ticks:
        logger.warning("bulk_insert_called_with_empty_list")
        return 0
    
    try:
        # Get raw psycopg2 connection
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Prepare data for COPY
        # Match the exact order of columns in the database
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
        
        # Create CSV-like string buffer
        buffer = io.StringIO()
        
        for tick in ticks:
            row_data = []
            
            for col in columns:
                value = tick.get(col)
                
                # Handle None/NULL
                if value is None:
                    row_data.append('\\N')
                
                # Handle arrays (convert Python list to PostgreSQL array format)
                elif col in ['bid_prices', 'bid_quantities', 'bid_orders', 
                           'ask_prices', 'ask_quantities', 'ask_orders']:
                    if isinstance(value, list):
                        # Convert to PostgreSQL array format: {val1,val2,val3}
                        array_str = '{' + ','.join(str(v) if v is not None else 'NULL' for v in value) + '}'
                        row_data.append(array_str)
                    else:
                        row_data.append('\\N')
                
                # Handle booleans
                elif isinstance(value, bool):
                    row_data.append('t' if value else 'f')
                
                # Handle timestamps
                elif isinstance(value, datetime):
                    row_data.append(value.isoformat())
                
                # Handle strings (escape tabs and newlines)
                elif isinstance(value, str):
                    escaped = value.replace('\t', '\\t').replace('\n', '\\n')
                    row_data.append(escaped)
                
                # Everything else as string
                else:
                    row_data.append(str(value))
            
            # Write tab-separated row
            buffer.write('\t'.join(row_data) + '\n')
        
        # Reset buffer position
        buffer.seek(0)
        
        # Execute COPY command
        cursor.copy_from(
            buffer,
            'ticks',
            columns=columns,
            null='\\N'
        )
        
        # Commit transaction
        conn.commit()
        
        rows_inserted = cursor.rowcount
        
        logger.info(
            "bulk_insert_successful",
            rows_inserted=rows_inserted,
            batch_size=len(ticks)
        )
        
        # Cleanup
        cursor.close()
        conn.close()
        
        return rows_inserted
    
    except Exception as e:
        logger.error(
            "bulk_insert_failed",
            error=str(e),
            batch_size=len(ticks)
        )
        
        # Try fallback method using SQLAlchemy
        try:
            logger.warning("attempting_fallback_insert_method")
            return _bulk_insert_fallback(ticks)
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
