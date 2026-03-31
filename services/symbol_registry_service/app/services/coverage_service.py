# app/services/coverage_service.py

from datetime import time

import yfinance as yf
import requests
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import SymbolRegistry, SymbolSourceMapping
from app.services.alpha_vantage_adapter import get_alpha_symbol

ALPHA_API_KEY = "067K1WAFQU3TIVTQ"
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

def is_index(symbol_obj) -> bool:
    return symbol_obj.instrument_type == "index"
def check_symbol_in_yfinance(symbol: str) -> bool:
    cached = get_cached(symbol, "yfinance")
    if cached is not None:
        return cached

    try:
        data = yf.Ticker(symbol).history(period="5d")

        valid = not data.empty
        set_cache(symbol, "yfinance", valid)

        return valid

    except:
        return False


logger = logging.getLogger("coverage_service")


import requests
coverage_cache = {}

def get_cached(symbol, provider):
    return coverage_cache.get(f"{provider}:{symbol}")

def set_cache(symbol, provider, value):
    coverage_cache[f"{provider}:{symbol}"] = value

import time



import requests

INGESTION_SERVICE_URL = "http://127.0.0.1:9000"  # adjust port


def check_via_api(source, symbol):
    try:
        url = f"{INGESTION_SERVICE_URL}/check/{source}/{symbol}"

        res = requests.get(url, timeout=3)

        print(res.json())

        if res.status_code != 200:
            return False

        return res.json().get("available", False)

    except Exception:
        return False


def update_single_symbol(db, mapping):

    symbol = db.query(SymbolRegistry).filter_by(
        symbol_id=mapping.symbol_id
    ).first()

    if not symbol:
        return mapping

    base_symbol = symbol.base_symbol
    country = symbol.country

    alpha_symbol = get_alpha_symbol(base_symbol, country)
    massive_symbol = base_symbol
    yfinance_symbol = mapping.yfinance_symbol or base_symbol

    # 🔥 API CALLS INSTEAD OF DIRECT ADAPTERS
    alpha_available = check_via_api("alpha_vantage", alpha_symbol)
    massive_available = check_via_api("massive", massive_symbol)
    yfinance_available = check_via_api("yfinance", yfinance_symbol)

    # Update mapping
    mapping.alpha_vantage_symbol = alpha_symbol if alpha_available else None
    mapping.massive_symbol = massive_symbol if massive_available else None
    mapping.yfinance_symbol = yfinance_symbol if yfinance_available else None

    # Coverage classification
    sources = {
        "alpha_vantage": alpha_available,
        "massive": massive_available,
        "yfinance": yfinance_available
    }

    available_sources = [k for k, v in sources.items() if v]

    if len(available_sources) == 0:
        mapping.coverage_type = "no_source"
    elif len(available_sources) == 1:
        mapping.coverage_type = "single_source"
    else:
        mapping.coverage_type = "multi_source"

    return mapping

def update_all_symbols():

    logger.info("[COVERAGE BATCH] Starting coverage update")

    start_time = time.time()
    db = SessionLocal()

    success = 0
    failed = 0
    results = []

    try:
        mappings = db.query(SymbolSourceMapping).all()
        total = len(mappings)

        logger.info(f"[COVERAGE BATCH] Total symbols to process: {total}")

        for mapping in mappings:

            symbol_id = mapping.symbol_id
            logger.info(f"[PROCESSING] {symbol_id}")

            try:
                # 🔥 Update logic (new version)
                update_single_symbol(db, mapping)

                # 🔹 Determine available sources
                sources = {
                    "alpha_vantage": bool(mapping.alpha_vantage_symbol),
                    "massive": bool(mapping.massive_symbol),
                    "yfinance": bool(mapping.yfinance_symbol)
                }

                available_sources = [k for k, v in sources.items() if v]

                # 🔹 Coverage classification
                if len(available_sources) == 0:
                    coverage_type = "no_source"
                elif len(available_sources) == 1:
                    coverage_type = "single_source"
                else:
                    coverage_type = "multi_source"

                mapping.coverage_type = coverage_type

                # 🔹 Append result
                results.append({
                    "symbol_id": symbol_id,
                    "coverage": coverage_type,
                    "sources": available_sources
                })

                success += 1

                logger.info(
                    f"[SUCCESS] {symbol_id} → {coverage_type} | "
                    f"sources={available_sources}"
                )

            except Exception as e:
                failed += 1

                logger.error(
                    f"[FAILED] {symbol_id} | Error: {str(e)}"
                )

        db.commit()
        logger.info("[COVERAGE BATCH] DB commit successful")

    except Exception as e:
        logger.exception(f"[BATCH FAILURE] {str(e)}")
        db.rollback()

    finally:
        db.close()
        logger.info("[COVERAGE BATCH] DB session closed")

    end_time = time.time()

    logger.info(
        f"[COVERAGE SUMMARY] total={total}, success={success}, "
        f"failed={failed}, time_taken={end_time - start_time:.2f}s"
    )

    return results