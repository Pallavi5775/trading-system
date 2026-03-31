# app/ingestion/adapters/alpha_vantage_adapter.py

import requests
import pandas as pd
import logging
from datetime import datetime

ALPHA_API_KEY = "LVGYHAQU8S3HKY06"

logger = logging.getLogger("alpha_adapter")


class AlphaVantageAdapter:

    def fetch_ohlc(self, symbol):

        logger.info(f"[ALPHA] Fetching data for symbol: {symbol}")

        url = "https://www.alphavantage.co/query"

        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": ALPHA_API_KEY,
            "outputsize": "full"
        }

        try:
            response = requests.get(url, params=params, timeout=10)
            data = response.json()

            logger.info(f"[ALPHA RAW RESPONSE] {data}")

        except Exception as e:
            logger.exception(f"[ALPHA ERROR] Request failed for {symbol}")
            raise Exception("Alpha request failed")

        # 🔥 Handle API issues

        if "Note" in data:
            logger.error(f"[ALPHA RATE LIMIT] {data['Note']}")
            raise Exception("Alpha rate limit hit")

        if "Error Message" in data:
            logger.error(f"[ALPHA INVALID SYMBOL] {symbol}")
            raise Exception("Invalid Alpha symbol")

        if "Time Series (Daily)" not in data:
            logger.error(f"[ALPHA NO DATA] {symbol}")
            raise Exception("No data returned from Alpha")

        time_series = data["Time Series (Daily)"]

        rows = []

        for date, values in time_series.items():
            rows.append({
                "timestamp": pd.to_datetime(date),
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "adj_close": float(values["5. adjusted close"]),
                "volume": float(values["6. volume"]),
                "data_source": "alpha_vantage",
                "ingestion_time": datetime.utcnow()
            })

        df = pd.DataFrame(rows)

        if df.empty:
            logger.warning(f"[ALPHA EMPTY DF] {symbol}")
            raise Exception("Alpha returned empty dataframe")

        logger.info(f"[ALPHA SUCCESS] {symbol} → {len(df)} rows fetched")

        return df
    
    def check_symbol(self, symbol):

        logger.info(f"[ALPHA CHECK] Checking symbol: {symbol}")

        url = "https://www.alphavantage.co/query"

        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": ALPHA_API_KEY
        }

        try:
            response = requests.get(url, params=params, timeout=3)
            data = response.json()

        except Exception as e:
            logger.warning(f"[ALPHA CHECK ERROR] {symbol} | {str(e)}")
            return False

        # ❌ Rate limit
        if "Note" in data or "Information" in data:
            logger.warning(f"[ALPHA CHECK RATE LIMIT] {symbol}")
            return False

        # ❌ Invalid symbol
        if "Error Message" in data:
            logger.warning(f"[ALPHA CHECK INVALID] {symbol}")
            return False

        # ❌ No data
        if "Time Series (Daily)" not in data:
            logger.warning(f"[ALPHA CHECK NO DATA] {symbol}")
            return False

        # ✅ Valid
        logger.info(f"[ALPHA CHECK SUCCESS] {symbol}")
        return True