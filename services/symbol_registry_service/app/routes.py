from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import SymbolRegistry, SymbolSourceMapping
from app.schemas import SymbolCreate, SourceMappingCreate
from app.services.coverage_scheduler import update_all_symbols
router = APIRouter()


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
            "alpha_vantage": mapping.alpha_vantage_symbol if mapping else None
        }
    }


# Get Available Sources
@router.get("/symbols/{symbol_id}/sources")
def get_sources(symbol_id: str, db: Session = Depends(get_db)):

    mapping = db.query(SymbolSourceMapping).filter_by(symbol_id=symbol_id).first()

    if not mapping:
        raise HTTPException(status_code=404, detail="Mapping not found")

    sources = []

    if mapping.yfinance_symbol:
        sources.append("yfinance")

    if mapping.alpha_vantage_symbol:
        sources.append("alpha_vantage")

    return {"available_sources": sources}


# Resolve Symbol
@router.get("/resolve/{symbol_id}")
def resolve_symbol(symbol_id: str, db: Session = Depends(get_db)):

    mapping = db.query(SymbolSourceMapping).filter_by(symbol_id=symbol_id).first()

    if not mapping:
        raise HTTPException(status_code=404, detail="Mapping not found")

    return {
        "symbol_id": symbol_id,
        "yfinance": mapping.yfinance_symbol,
        "alpha_vantage": mapping.alpha_vantage_symbol
    }






@router.post("/coverage/run")
def run_coverage():

    results = update_all_symbols()

    return {
        "message": "Coverage updated",
        "count": len(results),
        "data": results[:5]
    }