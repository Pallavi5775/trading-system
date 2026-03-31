from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import SymbolRegistry, SymbolSourceMapping
from app.schemas import SymbolCreate, SourceMappingCreate
from app.services.coverage_scheduler import update_all_symbols
from app.services.alpha_vantage_adapter import get_alpha_symbol
router = APIRouter()
import logging

logger = logging.getLogger("coverage_service")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Add Symbol
@router.post("/symbols")
def add_symbol(symbol: SymbolCreate, db: Session = Depends(get_db)):

    existing = db.query(SymbolRegistry).filter_by(symbol_id=symbol.symbol_id).first()
    if existing:
        raise HTTPException(status_code=400, detail="Symbol already exists")

    db_symbol = SymbolRegistry(**symbol.dict())
    db.add(db_symbol)
    db.commit()

    return {"message": "Symbol added successfully"}


# Add Mapping
@router.post("/mapping")
def add_mapping(mapping: SourceMappingCreate, db: Session = Depends(get_db)):

    db_mapping = SymbolSourceMapping(**mapping.dict())
    db.add(db_mapping)
    db.commit()

    return {"message": "Mapping added successfully"}


# Get Symbol Details
@router.get("/symbols/{symbol_id}")
def get_symbol(symbol_id: str, db: Session = Depends(get_db)):

    symbol = db.query(SymbolRegistry).filter_by(symbol_id=symbol_id).first()

    if not symbol:
        raise HTTPException(status_code=404, detail="Symbol not found")

    mapping = db.query(SymbolSourceMapping).filter_by(symbol_id=symbol_id).first()

    return {
        "symbol": {
            "symbol_id": symbol.symbol_id,
            "base_symbol": symbol.base_symbol,
            "exchange": symbol.exchange,
            "currency": symbol.currency,
            "sector": symbol.sector
        },
        "mapping": {
            "yfinance": mapping.yfinance_symbol if mapping else None,
            "alpha_vantage": mapping.alpha_vantage_symbol if mapping else None,
            "massive": mapping.massive_symbol if mapping else None,
            "coverage_type": mapping.coverage_type if mapping else None
        }
    }

# Get Available Sources
@router.get("/symbols/{symbol_id}/sources")
def get_sources(symbol_id: str, db: Session = Depends(get_db)):

    mapping = db.query(SymbolSourceMapping).filter_by(symbol_id=symbol_id).first()

    if not mapping:
        raise HTTPException(status_code=404, detail="Mapping not found")

    sources = []

    if mapping.alpha_vantage_symbol:
        sources.append("alpha_vantage")

    if mapping.massive_symbol:
        sources.append("massive")

    if mapping.yfinance_symbol:
        sources.append("yfinance")

    return {"available_sources": sources}



@router.get("/resolve/{symbol_id}")
def resolve_symbol(symbol_id: str, db: Session = Depends(get_db)):

    mapping = db.query(SymbolSourceMapping).filter_by(symbol_id=symbol_id).first()

    if not mapping:
        raise HTTPException(status_code=404, detail="Mapping not found")

    symbol = db.query(SymbolRegistry).filter_by(symbol_id=symbol_id).first()

    # 🔹 Auto Alpha symbol
    alpha_symbol = mapping.alpha_vantage_symbol
    if not alpha_symbol:
        alpha_symbol = get_alpha_symbol(
            symbol.base_symbol,
            symbol.country
        )

    # 🔹 Massive symbol (usually same as base for US equities)
    massive_symbol = mapping.massive_symbol or symbol.base_symbol

    return {
        "symbol_id": symbol_id,
        "alpha_vantage": alpha_symbol,
        "massive": massive_symbol,
        "yfinance": mapping.yfinance_symbol
    }




@router.post("/coverage/run")
def run_coverage():

    logger.info("[API] Coverage run triggered")

    results = update_all_symbols()

    logger.info("[API] Coverage run completed")

    return {
        "message": "Coverage updated",
        "count": len(results),
        "data": results
    }