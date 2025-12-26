"""
Configuration Management
Loads and validates environment variables
"""

import os
from typing import List
from dotenv import load_dotenv
import psycopg2
import structlog

# Load environment variables from .env file
load_dotenv()

logger = structlog.get_logger()


class Config:
    """Application configuration from environment variables"""
    
    def __init__(self):
        # Redis
        self.REDIS_URL: str = self._get_required("REDIS_URL")
        
        # RabbitMQ
        self.RABBITMQ_URL: str = self._get_required("RABBITMQ_URL")
        
        # KiteConnect
        self.KITE_API_KEY: str = self._get_required("KITE_API_KEY")
        
        # Database (for instruments)
        self.DATABASE_URL: str = self._get_required("DATABASE_URL")
        
        # Instruments to track - loaded from database
        self.INSTRUMENTS: List[int] = self._load_instruments_from_db()
        
        # Logging
        self.LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
        
        # Validate configuration
        self._validate()
    
    def _get_required(self, key: str) -> str:
        """Get required environment variable or raise error"""
        value = os.getenv(key)
        if not value:
            raise EnvironmentError(
                f"Required environment variable '{key}' is not set. "
                f"Please check your .env file."
            )
        return value
    
    def _load_instruments_from_db(self) -> List[int]:
        """Load active instruments from database"""
        try:
            conn = psycopg2.connect(self.DATABASE_URL)
            cursor = conn.cursor()
            
            # Query active instruments
            cursor.execute("""
                SELECT instrument_token 
                FROM instruments 
                WHERE is_active = TRUE
                ORDER BY instrument_token
            """)
            
            tokens = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            if not tokens:
                logger.warning(
                    "no_active_instruments",
                    message="No instruments marked as active in database. "
                           "Run: UPDATE instruments SET is_active = TRUE WHERE name = 'NIFTY' AND segment IN ('NFO-OPT', 'NFO-FUT')"
                )
                raise ValueError("No active instruments found in database")
            
            logger.info(
                "instruments_loaded_from_db",
                count=len(tokens)
            )
            
            return tokens
            
        except psycopg2.Error as e:
            logger.error("database_connection_failed", error=str(e))
            # Fallback to .env if DB fails
            instruments_str = os.getenv("INSTRUMENTS", "")
            if instruments_str:
                logger.warning("falling_back_to_env_instruments")
                tokens = [int(t.strip()) for t in instruments_str.split(",") if t.strip()]
                return tokens
            raise EnvironmentError(
                f"Failed to load instruments from database and no INSTRUMENTS in .env. "
                f"Database error: {e}"
            )
    
    def _validate(self):
        """Validate configuration values"""
        # Validate Redis URL format
        if not self.REDIS_URL.startswith("redis://"):
            raise ValueError(f"Invalid REDIS_URL format: {self.REDIS_URL}")
        
        # Validate RabbitMQ URL format
        if not self.RABBITMQ_URL.startswith("amqp://"):
            raise ValueError(f"Invalid RABBITMQ_URL format: {self.RABBITMQ_URL}")
        
        # Validate instrument tokens
        if not self.INSTRUMENTS:
            raise ValueError("At least one instrument token must be specified")
        
        # Validate log level
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.LOG_LEVEL.upper() not in valid_levels:
            raise ValueError(
                f"Invalid LOG_LEVEL: {self.LOG_LEVEL}. "
                f"Must be one of: {', '.join(valid_levels)}"
            )
    
    def __repr__(self) -> str:
        return (
            f"Config(REDIS_URL={self.REDIS_URL}, "
            f"RABBITMQ_URL={self.RABBITMQ_URL[:20]}..., "
            f"INSTRUMENTS={len(self.INSTRUMENTS)} tokens, "
            f"LOG_LEVEL={self.LOG_LEVEL})"
        )


# Global config instance
config = Config()
