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

from routes import kite, health

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


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
