from pydantic import BaseModel
from typing import Optional


class SymbolCreate(BaseModel):
    symbol_id: str
    base_symbol: str
    asset_class: str
    exchange: str
    country: str
    currency: str
    timezone: str
    sector: str


class SymbolResponse(SymbolCreate):
    is_active: bool

    class Config:
        from_attributes = True


class SourceMappingCreate(BaseModel):
    symbol_id: str
    yfinance_symbol: Optional[str] = None
    alpha_vantage_symbol: Optional[str] = None


class SourceMappingResponse(SourceMappingCreate):
    class Config:
        from_attributes = True