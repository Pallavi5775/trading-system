from sqlalchemy import Column, DateTime, Float, Integer, String, Boolean, TIMESTAMP, func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class SymbolRegistry(Base):
    __tablename__ = "symbol_registry"
    __table_args__ = {"schema": "trading_system"}  # schema defined here

    symbol_id = Column(String, primary_key=True, index=True)
    base_symbol = Column(String, nullable=False)

    asset_class = Column(String)
    exchange = Column(String)
    country = Column(String)

    currency = Column(String)
    timezone = Column(String)

    sector = Column(String)

    is_active = Column(Boolean, default=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())


class SymbolSourceMapping(Base):
    __tablename__ = "symbol_source_mapping"
    __table_args__ = {"schema": "trading_system"}  # schema

    symbol_id = Column(String, primary_key=True, index=True)

    yfinance_symbol = Column(String, nullable=True)
    alpha_vantage_symbol = Column(String, nullable=True)
    massive_symbol= Column(String, nullable=True)

    # ADD THESE (for coverage system)
    coverage_type = Column(String)
    primary_source = Column(String)
    fallback_source = Column(String)

# app/models/alpha_symbol_search.py

class AlphaSymbolSearch(Base):
    __tablename__ = "alpha_symbol_search"

    id = Column(Integer, primary_key=True, autoincrement=True)

    query = Column(String)   # searched keyword (e.g., TESLA)
    symbol = Column(String)
    name = Column(String)
    type = Column(String)
    region = Column(String)
    currency = Column(String)
    timezone = Column(String)
    match_score = Column(Float)

    created_at = Column(DateTime)    