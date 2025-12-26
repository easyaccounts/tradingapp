"""
SQLAlchemy Models for Database
Matches TimescaleDB schema
"""

from sqlalchemy import Column, Integer, BigInteger, String, Boolean, TIMESTAMP, ARRAY, Numeric
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class Tick(Base):
    """
    Ticks table model
    Stores high-frequency market tick data
    """
    __tablename__ = 'ticks'
    
    # Timestamps (composite primary key with instrument_token)
    time = Column(TIMESTAMP(timezone=True), primary_key=True, nullable=False)
    last_trade_time = Column(TIMESTAMP(timezone=True))
    
    # Instrument identification
    instrument_token = Column(Integer, primary_key=True, nullable=False)
    trading_symbol = Column(String)
    exchange = Column(String)
    instrument_type = Column(String)
    
    # Price data
    last_price = Column(Numeric(12, 2))
    last_traded_quantity = Column(Integer)
    average_traded_price = Column(Numeric(12, 2))
    
    # Volume & OI
    volume_traded = Column(BigInteger)
    oi = Column(BigInteger)
    oi_day_high = Column(BigInteger)
    oi_day_low = Column(BigInteger)
    
    # OHLC (Day candle)
    day_open = Column(Numeric(12, 2))
    day_high = Column(Numeric(12, 2))
    day_low = Column(Numeric(12, 2))
    day_close = Column(Numeric(12, 2))
    
    # Change
    change = Column(Numeric(12, 2))
    change_percent = Column(Numeric(8, 4))
    
    # Order book totals
    total_buy_quantity = Column(BigInteger)
    total_sell_quantity = Column(BigInteger)
    
    # Market Depth - Bids (5 levels)
    # PostgreSQL ARRAY type
    bid_prices = Column(ARRAY(Numeric(12, 2), dimensions=1))
    bid_quantities = Column(ARRAY(Integer, dimensions=1))
    bid_orders = Column(ARRAY(Integer, dimensions=1))
    
    # Market Depth - Asks (5 levels)
    ask_prices = Column(ARRAY(Numeric(12, 2), dimensions=1))
    ask_quantities = Column(ARRAY(Integer, dimensions=1))
    ask_orders = Column(ARRAY(Integer, dimensions=1))
    
    # Metadata
    tradable = Column(Boolean, default=True)
    mode = Column(String)
    
    # Derived fields
    bid_ask_spread = Column(Numeric(12, 2))
    mid_price = Column(Numeric(12, 2))
    order_imbalance = Column(BigInteger)
    
    def __repr__(self):
        return (
            f"<Tick(time={self.time}, "
            f"instrument_token={self.instrument_token}, "
            f"last_price={self.last_price})>"
        )


class Instrument(Base):
    """
    Instruments table model
    Stores metadata for all tradable instruments
    """
    __tablename__ = 'instruments'
    
    # Primary identification
    instrument_token = Column(Integer, primary_key=True)
    exchange_token = Column(Integer)
    
    # Symbol and naming
    trading_symbol = Column(String, nullable=False)
    name = Column(String)
    
    # Classification
    exchange = Column(String, nullable=False)
    segment = Column(String)
    instrument_type = Column(String)
    
    # Derivatives metadata
    expiry = Column(TIMESTAMP(timezone=False))
    strike = Column(Numeric(12, 2))
    
    # Trading parameters
    tick_size = Column(Numeric(10, 4))
    lot_size = Column(Integer)
    
    # Metadata
    last_updated = Column(TIMESTAMP(timezone=True), default=datetime.now)
    
    def __repr__(self):
        return (
            f"<Instrument(token={self.instrument_token}, "
            f"symbol={self.trading_symbol}, "
            f"exchange={self.exchange})>"
        )
