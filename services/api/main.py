"""
Trading Platform API - Main Application
FastAPI-based REST API for Kite authentication and system health monitoring
"""

import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app
from redis import Redis
import asyncpg

from routes import kite, health, orderflow

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def load_instruments_to_redis():
    """Load instruments from PostgreSQL to Redis on startup if Redis is empty"""
    try:
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        redis_client = Redis.from_url(redis_url, decode_responses=True)
        
        # Check if Redis already has instruments
        instrument_count = 0
        for key in redis_client.scan_iter("instrument:*", count=10):
            instrument_count += 1
            if instrument_count > 10:  # Found some instruments, assume populated
                break
        
        if instrument_count > 10:
            logger.info(f"Redis already has instruments (found {instrument_count}+), skipping load")
            redis_client.close()
            return
        
        logger.info("Redis has no instruments, loading from PostgreSQL...")
        
        # Connect to PostgreSQL
        database_url = os.getenv("DATABASE_URL", "")
        if database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql://", 1)
        
        conn = await asyncpg.connect(database_url)
        
        # Fetch all instruments from database
        instruments = await conn.fetch("""
            SELECT 
                instrument_token, exchange_token, trading_symbol, name,
                exchange, segment, instrument_type, expiry, strike,
                tick_size, lot_size
            FROM instruments
        """)
        
        logger.info(f"Fetched {len(instruments)} instruments from PostgreSQL")
        
        # Load into Redis
        loaded_count = 0
        for instrument in instruments:
            key = f"instrument:{instrument['instrument_token']}"
            
            data = {
                'tradingsymbol': instrument['trading_symbol'] or '',
                'exchange': instrument['exchange'] or '',
                'instrument_type': instrument['instrument_type'] or '',
                'segment': instrument['segment'] or '',
                'name': instrument['name'] or '',
                'expiry': str(instrument['expiry']) if instrument['expiry'] else '',
                'strike': str(instrument['strike']) if instrument['strike'] else '0',
                'tick_size': str(instrument['tick_size']) if instrument['tick_size'] else '0',
                'lot_size': str(instrument['lot_size']) if instrument['lot_size'] else '0',
                'exchange_token': str(instrument['exchange_token']) if instrument['exchange_token'] else '0'
            }
            
            redis_client.hset(key, mapping=data)
            loaded_count += 1
        
        await conn.close()
        redis_client.close()
        
        logger.info(f"✓ Successfully loaded {loaded_count} instruments into Redis")
        
    except Exception as e:
        logger.error(f"Failed to load instruments to Redis: {e}")
        # Don't fail startup, just log the error


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager for startup and shutdown events"""
    # Startup
    logger.info("Starting Trading Platform API...")
    logger.info(f"Environment: {os.getenv('ENVIRONMENT', 'development')}")
    
    # Validate required environment variables
    required_vars = ["KITE_API_KEY", "KITE_API_SECRET", "REDIS_URL", "DATABASE_URL"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        raise EnvironmentError(f"Missing environment variables: {missing_vars}")
    
    logger.info("All required environment variables present")
    
    # Load instruments from PostgreSQL to Redis
    await load_instruments_to_redis()
    
    logger.info("API startup complete")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Trading Platform API...")


# Initialize FastAPI application
app = FastAPI(
    title="Trading Platform API",
    description="Production-grade API for trading data ingestion from KiteConnect",
    version="1.0.0",
    lifespan=lifespan
)

# CORS Middleware - Production-ready with environment variable control
allowed_origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")
if allowed_origins == ["*"] and os.getenv("ENVIRONMENT") == "production":
    logger.warning("CORS set to allow all origins in production - this is insecure!")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router, tags=["health"])
app.include_router(kite.router, prefix="/api/kite", tags=["kite"])
app.include_router(orderflow.router, prefix="/api", tags=["orderflow"])

# Mount Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.get("/")
async def root():
    """Root endpoint - API status check"""
    return {
        "status": "API is running",
        "version": "1.0.0",
        "service": "Trading Platform API",
        "environment": os.getenv("ENVIRONMENT", "development")
    }


if __name__ == "__main__":
    import uvicorn
    
    # Run with uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
        reload=os.getenv("ENVIRONMENT") == "development"
    )
