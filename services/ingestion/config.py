"""
Configuration Management
Loads and validates environment variables
"""

import os
from typing import List
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Application configuration from environment variables"""
    
    def __init__(self):
        # Redis
        self.REDIS_URL: str = self._get_required("REDIS_URL")
        
        # RabbitMQ
        self.RABBITMQ_URL: str = self._get_required("RABBITMQ_URL")
        
        # KiteConnect
        self.KITE_API_KEY: str = self._get_required("KITE_API_KEY")
        
        # Instruments to track (comma-separated)
        instruments_str = self._get_required("INSTRUMENTS")
        self.INSTRUMENTS: List[int] = self._parse_instruments(instruments_str)
        
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
    
    def _parse_instruments(self, instruments_str: str) -> List[int]:
        """Parse comma-separated instrument tokens"""
        try:
            tokens = [int(token.strip()) for token in instruments_str.split(",") if token.strip()]
            if not tokens:
                raise ValueError("No instruments specified")
            return tokens
        except ValueError as e:
            raise ValueError(
                f"Invalid INSTRUMENTS format: {instruments_str}. "
                f"Expected comma-separated integers. Error: {e}"
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
