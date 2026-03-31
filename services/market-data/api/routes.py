from fastapi import APIRouter, HTTPException
import pandas as pd
import requests
import json
# Adapters
from ingestion.adapters.yfinance_adapter import YFinanceAdapter
from ingestion.adapters.alpha_vantage_adapter import AlphaVantageAdapter
from storage.market_store_service import store_market_data_with_versioning
# Pipeline
from normalization.normalizer import normalize
from features.feature_engine import compute_returns, compute_volatility
from validation.validator import validate
from ingestion.adapters.massive_adapter import MassiveAdapter
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
    "alpha_vantage": AlphaVantageAdapter(),
    "massive": MassiveAdapter(),
    "yfinance": YFinanceAdapter()
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


        massive = mapping.get("massive")

        coverage = {
            "alpha_vantage": alpha_vantage,
            "massive": massive,
            "yfinance": yfinance
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

    # Priority order (your requirement)
    priority_order = ["alpha_vantage", "massive", "yfinance"]

    

    successful_data = []

    # Step 1: Try all providers (not just primary/fallback)
    for source in priority_order:
        symbol = coverage.get(source)

        if not symbol:
            logger.info(f"{source} not available for {symbol_id}")
            continue

        try:
            logger.info(f"Trying source: {source} for {symbol_id}")

            df = adapters[source].fetch_ohlc(symbol)

            if df is None or df.empty:
                raise Exception("Empty dataframe")

            df["source_used"] = source
            df = enrich_dataframe(df, symbol_id, source)

            successful_data.append((source, df))

            logger.info(f"{source} SUCCESS for {symbol_id}")

        except Exception as e:
            logger.warning(f"{source} FAILED for {symbol_id}: {str(e)}")

    # Step 2: Decide based on results

    if not successful_data:
        logger.error(f"All providers failed for {symbol_id}")
        return pd.DataFrame()  # prevent crash

    # CASE 1: Reconciliation (if 2+ sources available)
    if len(successful_data) >= 2:
        try:
            logger.info(f"Attempting reconciliation for {symbol_id}")

            # Take top 2 sources (based on priority)
            df1 = successful_data[0][1]
            df2 = successful_data[1][1]

            df = reconcile_data(df1, df2)
            df["source_used"] = "reconciled"

            logger.info(f"Reconciliation SUCCESS for {symbol_id}")
            return df

        except Exception as e:
            logger.warning(f"Reconciliation FAILED for {symbol_id}: {str(e)}")

    # CASE 2: Fallback to best available (first success)
    best_source, best_df = successful_data[0]

    logger.info(f"Using best available source: {best_source} for {symbol_id}")

    return best_df

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


def make_json_safe(df, symbol_id):
    logger.info(f"Final JSON sanitization for: {symbol_id}")

    # Replace inf → NaN
    df = df.replace([np.inf, -np.inf], np.nan)

    # Sort
    df = df.sort_values("timestamp")

    #  Forward fill
    fill_cols = ["open", "high", "low", "close", "adj_close", "volume"]
    df[fill_cols] = df[fill_cols].ffill().bfill()

     # Convert timestamps
    if "timestamp" in df.columns:
        df["timestamp"] = df["timestamp"].astype(str)

    # 🔹 Handle derived columns
    if "log_return" in df.columns:
        df["log_return"] = df["log_return"].replace([np.inf, -np.inf], np.nan).fillna(0)

    if "volatility" in df.columns:
        df["volatility"] = df["volatility"].replace([np.inf, -np.inf], np.nan).fillna(0)

    # CRITICAL STEP (this fixes your issue)
    df = df.astype(object).where(pd.notnull(df), None)

    return df



"""Basic connection example.
"""

import redis

r = redis.Redis(
    host='redis-18652.c267.us-east-1-4.ec2.cloud.redislabs.com',
    port=18652,
    decode_responses=True,
    username="default",
    password="kZqrqVP7sC7YkkIgiNPtAs8M3NK5Gf0r",
)


def json_serializer(obj):
    import pandas as pd
    import numpy as np

    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()

    if isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
        return None

    if pd.isna(obj):
        return None

    raise TypeError(f"Type {type(obj)} not serializable")



@router.get("/fetch/{symbol_id}")
def fetch_data(symbol_id: str):

    logger.info(f"API request received: /fetch/{symbol_id}")

    df = fetch_market_data(symbol_id)

    df = normalize(df, symbol_id)
    if "symbol_id" not in df.columns:
      df["symbol_id"] = symbol_id
    df = compute_returns(df)
    df = compute_volatility(df)
    df = validate(df)
    df = make_json_safe(df, symbol_id)
    store_market_data_with_versioning(df)

    latest = df.tail(10).to_dict(orient="records")
    

    r.set(
        f"market:{symbol_id}",
        json.dumps(latest, default=json_serializer)
    )

    for record in latest:
       r.publish("market_updates", json.dumps(record, default=json_serializer))

    logger.info(f"Published latest market data for {symbol_id}")

    logger.info(f"Returning response for: {symbol_id}")
    
    return df.tail(5).to_dict(orient="records")



@router.post("/check/batch")
def check_batch(payload: dict):

    adapter_map = {
    "alpha_vantage": AlphaVantageAdapter(),
    "massive": MassiveAdapter(),
    "yfinance": YFinanceAdapter()
}

    results = {}

    for source, symbols in payload.items():
        adapter = adapter_map[source]

        results[source] = {}

        for symbol in symbols:
            try:
                results[source][symbol] = adapter.check_symbol(symbol)
            except:
                results[source][symbol] = False

    return results
@router.get("/check/{source}/{symbol}")
def check_symbol(source: str, symbol: str):

    adapter_map = {
        "alpha_vantage": AlphaVantageAdapter(),
        "massive": MassiveAdapter(),
        "yfinance": YFinanceAdapter()
    }

    if source not in adapter_map:
        raise HTTPException(status_code=400, detail="Invalid source")

    try:
        adapter = adapter_map[source]

        # 🔹 Use lightweight check (IMPORTANT)
        is_available = adapter.check_symbol(symbol)

        return {
            "source": source,
            "symbol": symbol,
            "available": is_available
        }

    except Exception as e:
        return {
            "source": source,
            "symbol": symbol,
            "available": False,
            "error": str(e)
        }