from fastapi import APIRouter, HTTPException
import pandas as pd
import requests

# Adapters
from ingestion.adapters.yfinance_adapter import YFinanceAdapter
from ingestion.adapters.alpha_vantage_adapter import AlphaVantageAdapter

# Pipeline
from normalization.normalizer import normalize
from features.feature_engine import compute_returns, compute_volatility
from validation.validator import validate
from reconciliation.reconciler import reconcile_data
router = APIRouter()

# Registry Service URL (adjust if needed)
SYMBOL_REGISTRY_URL = "http://127.0.0.1:8000"

import logging

logger = logging.getLogger("market_data_service")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
# 🔹 Adapter Map
adapters = {
    "yfinance": YFinanceAdapter(),
    "alpha_vantage": AlphaVantageAdapter()
}


# Resolve symbol from registry
def resolve_symbol(symbol_id: str):
    logger.info(f"Resolving symbol: {symbol_id}")

    try:
        response = requests.get(f"{SYMBOL_REGISTRY_URL}/resolve/{symbol_id}")

        if response.status_code != 200:
            logger.error(f"Symbol resolution failed: {symbol_id} | Status: {response.status_code}")
            raise HTTPException(status_code=404, detail="Symbol not found")

        data = response.json()
        logger.info(f"Resolved symbol: {symbol_id} -> {data}")
        return data

    except Exception as e:
        logger.exception(f"Exception in resolve_symbol: {symbol_id} | Error: {str(e)}")
        raise HTTPException(status_code=404, detail="Symbol resolution failed")


# Fetch coverage info
def get_coverage(symbol_id: str):
    logger.info(f"Fetching coverage for: {symbol_id}")

    try:
        response = requests.get(f"{SYMBOL_REGISTRY_URL}/symbols/{symbol_id}")

        if response.status_code != 200:
            logger.error(f"Coverage fetch failed: {symbol_id}")
            raise HTTPException(status_code=404, detail="Symbol not found in registry")

        data = response.json()
        mapping = data.get("mapping", {})

        # Extract available sources
        yfinance = mapping.get("yfinance")
        alpha_vantage = mapping.get("alpha_vantage")

        # Dynamic selection logic
        primary = None
        fallback = None

        if yfinance and alpha_vantage:
            primary = "yfinance"
            fallback = "alpha_vantage"
        elif yfinance:
            primary = "yfinance"
        elif alpha_vantage:
            primary = "alpha_vantage"

        coverage = {
            "primary": primary,
            "fallback": fallback,
            "yfinance": yfinance,
            "alpha_vantage": alpha_vantage
        }

        logger.info(f"Coverage for {symbol_id}: {coverage}")
        return coverage

    except Exception as e:
        logger.exception(f"Exception in get_coverage: {symbol_id} | Error: {str(e)}")
        raise HTTPException(status_code=500, detail="Coverage fetch failed")

    except Exception as e:
        logger.exception(f"Exception in get_coverage: {symbol_id} | Error: {str(e)}")
        raise HTTPException(status_code=500, detail="Coverage fetch failed")
def enrich_dataframe(df, symbol_id, source):
    logger.info(f"Enriching dataframe for: {symbol_id}")

    # Ensure required columns exist
    required_cols = ["timestamp", "open", "high", "low", "close", "volume"]

    for col in required_cols:
        if col not in df.columns:
            logger.error(f"Missing column: {col} for {symbol_id}")
            raise HTTPException(500, f"Missing column {col}")

    # Fix adj_close
    if "adj_close" not in df.columns:
        logger.warning(f"adj_close missing for {symbol_id}, using close instead")
        df["adj_close"] = df["close"]

    # 🔹 Add metadata
    df["symbol_id"] = symbol_id
    df["data_source"] = source

    return df

# MAIN FETCH FUNCTION (CORE LOGIC)
def fetch_market_data(symbol_id: str):

    logger.info(f"Starting fetch pipeline for: {symbol_id}")

    coverage = get_coverage(symbol_id)

    primary = coverage["primary"]
    fallback = coverage["fallback"]

    if not primary:
        logger.warning(f"No primary source configured for: {symbol_id}")
        raise HTTPException(status_code=400, detail="No primary source configured")

    # CASE 1: Reconciliation
    if primary == "yfinance" and fallback == "alpha_vantage":

        logger.info(f"Attempting reconciliation for: {symbol_id}")

        try:
            y_symbol = coverage["yfinance"]
            a_symbol = coverage["alpha_vantage"]

            df_y = adapters["yfinance"].fetch_ohlc(y_symbol)
            df_a = adapters["alpha_vantage"].fetch_ohlc(a_symbol)

            # FIX: handle empty alpha_vantage
            if df_a is None or df_a.empty:
                logger.warning("Alpha Vantage returned empty → skipping reconciliation")
                df_y["source_used"] = "yfinance"
                return df_y

            df = reconcile_data(df_y, df_a)
            df["source_used"] = "reconciled"

            logger.info(f"Reconciliation successful for: {symbol_id}")
            return df

        except Exception as e:
            logger.warning(f"Reconciliation skipped for {symbol_id}: {str(e)}")

    # CASE 2: Primary
    try:
        logger.info(f"Trying primary source ({primary}) for: {symbol_id}")

        symbol = coverage[primary]
        df = adapters[primary].fetch_ohlc(symbol)

        df["source_used"] = primary
        df = enrich_dataframe(df, symbol_id, primary)

        return df

    except Exception as e:
        logger.exception(f"Primary source failed: {primary} | {symbol_id} | Error: {str(e)}")

    # Fallback
    if fallback:
        try:
            logger.info(f"Trying fallback source ({fallback}) for: {symbol_id}")

            symbol = coverage[fallback]
            df = adapters[fallback].fetch_ohlc(symbol)

            df["source_used"] = fallback
            df = enrich_dataframe(df, symbol_id, fallback)

            return df
        except Exception as e:
            logger.exception(f"Fallback failed: {fallback} | {symbol_id} | Error: {str(e)}")

    logger.critical(f"All sources failed for: {symbol_id}")
    raise HTTPException(status_code=500, detail="No data available from any source")
def select_sources(symbol_id: str, coverage: dict):

    source_scores = {
        "yfinance": 90,        # fast + free
        "alpha_vantage": 70    # slower + rate-limited
    }

    available = {
        k: v for k, v in coverage.items()
        if k in source_scores and v is not None
    }

    if not available:
        raise HTTPException(400, "No sources available")

    # Sort by score
    sorted_sources = sorted(
        available.keys(),
        key=lambda x: source_scores[x],
        reverse=True
    )

    primary = sorted_sources[0]
    fallback = sorted_sources[1] if len(sorted_sources) > 1 else None

    logger.info(f"[SMART SELECTOR] {symbol_id} → {primary}, {fallback}")

    return primary, fallback

def flatten_columns(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join(col).strip() for col in df.columns.values]
    return df

def sanitize_dataframe(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            logger.warning(f"Column {col} has complex types, converting to string")
            df[col] = df[col].astype(str)
    return df
# API ENDPOINT

import numpy as np
import pandas as pd

import numpy as np
import pandas as pd

def make_json_safe(df, symbol_id):
    logger.info(f"Final JSON sanitization for: {symbol_id}")

    # Replace inf → NaN
    df = df.replace([np.inf, -np.inf], np.nan)

    # Sort
    df = df.sort_values("timestamp")

    #  Forward fill
    fill_cols = ["open", "high", "low", "close", "adj_close", "volume"]
    df[fill_cols] = df[fill_cols].ffill().bfill()

    # 🔹 Handle derived columns
    if "log_return" in df.columns:
        df["log_return"] = df["log_return"].replace([np.inf, -np.inf], np.nan).fillna(0)

    if "volatility" in df.columns:
        df["volatility"] = df["volatility"].replace([np.inf, -np.inf], np.nan).fillna(0)

    # CRITICAL STEP (this fixes your issue)
    df = df.astype(object).where(pd.notnull(df), None)

    return df

@router.get("/fetch/{symbol_id}")
def fetch_data(symbol_id: str):

    logger.info(f"API request received: /fetch/{symbol_id}")

    df = fetch_market_data(symbol_id)

    df = normalize(df, symbol_id)
    df = compute_returns(df)
    df = compute_volatility(df)
    df = validate(df)
    df = make_json_safe(df, symbol_id)

    logger.info(f"Returning response for: {symbol_id}")
    return df.tail(5).to_dict(orient="records")
