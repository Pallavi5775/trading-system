# app/services/coverage_service.py

import yfinance as yf
import requests
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import SymbolSourceMapping

ALPHA_API_KEY = "PIO88D64WECKACN2"


def check_symbol_in_yfinance(symbol: str) -> bool:
    try:
        data = yf.Ticker(symbol).history(period="5d")
        return not data.empty
    except:
        return False


def check_symbol_in_alpha(symbol: str) -> bool:
    try:
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": ALPHA_API_KEY
        }

        response = requests.get(url, params=params, timeout=5).json()

        return "Time Series (Daily)" in response

    except:
        return False


def classify_coverage(y_symbol, a_symbol):

    y = check_symbol_in_yfinance(y_symbol)
    a = check_symbol_in_alpha(a_symbol) if a_symbol else False

    if y and a:
        return "dual_source", "yfinance", "alpha_vantage"

    elif y:
        return "yfinance_only", "yfinance", None

    elif a:
        return "alpha_only", "alpha_vantage", None

    else:
        return "unsupported", None, None


def update_single_symbol(db: Session, mapping: SymbolSourceMapping):

    coverage, primary, fallback = classify_coverage(
        mapping.yfinance_symbol,
        mapping.alpha_vantage_symbol
    )

    mapping.coverage_type = coverage
    mapping.primary_source = primary
    mapping.fallback_source = fallback

    return mapping


def update_all_symbols():

    db = SessionLocal()

    mappings = db.query(SymbolSourceMapping).all()

    results = []

    for mapping in mappings:

        try:
            updated = update_single_symbol(db, mapping)

            results.append({
                "symbol_id": mapping.symbol_id,
                "coverage": mapping.coverage_type,
                "primary": mapping.primary_source,
                "fallback": mapping.fallback_source
            })

        except Exception as e:
            print(f"Error updating {mapping.symbol_id}: {e}")

    db.commit()
    db.close()

    return results