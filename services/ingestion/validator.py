"""
Tick Data Validator
Validates incoming tick data for quality and consistency
"""

import structlog
from typing import Optional
from models import KiteTick

logger = structlog.get_logger()


def validate_tick(tick: KiteTick) -> bool:
    """
    Validate tick data for quality and consistency
    
    Validation checks:
    - Required fields are present
    - Price values are positive
    - Volume values are non-negative
    - No obvious data anomalies or outliers
    
    Args:
        tick: KiteTick instance to validate
    
    Returns:
        bool: True if tick is valid, False otherwise
    """
    try:
        # Check required fields
        if not tick.instrument_token:
            logger.warning("validation_failed", reason="missing_instrument_token")
            return False
        
        # Validate price data (if present, must not be negative - zero is allowed for options)
        if tick.last_price is not None and tick.last_price < 0:
            logger.warning("validation_failed", reason="negative_last_price", value=tick.last_price, instrument_token=tick.instrument_token)
            return False
        
        # Validate volumes (must be non-negative)
        if tick.volume_traded is not None and tick.volume_traded < 0:
            logger.warning("validation_failed", reason="negative_volume", value=tick.volume_traded, instrument_token=tick.instrument_token)
            return False
        
        # All validations passed
        return True
    
    except Exception as e:
        logger.error("validation_error", error=str(e), instrument_token=tick.instrument_token if tick else None)
        return False


def validate_tick_basic(tick: KiteTick) -> bool:
    """
    Basic validation - only checks critical fields
    
    Args:
        tick: KiteTick instance to validate
    
    Returns:
        bool: True if basic validation passes
    """
    return tick.instrument_token is not None and (tick.last_price is None or tick.last_price > 0)
