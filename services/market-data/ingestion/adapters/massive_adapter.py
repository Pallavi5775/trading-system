# ingestion/adapters/massive_adapter.py

from datetime import datetime, timedelta

import httpx
import pandas as pd
import requests
import logging

logger = logging.getLogger("market_data_service")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
class MassiveAdapter:

    BASE_URL = "https://api.massive.com/v2/aggs/ticker"

    end = datetime.utcnow()
    start = end - timedelta(days=60)

    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    def fetch_ohlc(self, symbol, start=None, end=None):
        url = f"{self.BASE_URL}/{symbol}/range/1/day/{self.start_str}/{self.end_str}"

        params = {
            "apiKey": "Y_dGYbvRsZJn1KMzmtDj4_I1_rncfcU8"
        }

        res = httpx.get(url, params=params)
        data = res.json()

        if "results" not in data:
            raise Exception("Massive returned no data")

        df = pd.DataFrame(data["results"])

        df.rename(columns={
            "t": "timestamp",
            "o": "open",
            "h": "high",
            "l": "low",
            "c": "close",
            "v": "volume"
        }, inplace=True)

        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

        return df
    
    def check_symbol(self, symbol):

        end = datetime.utcnow()
        start = end - timedelta(days=60)

        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")

        try:
            url = f"{self.BASE_URL}/{symbol}/range/1/day/{start_str}/{end_str}"

            res = requests.get(
                url,
                params={"apiKey": "Y_dGYbvRsZJn1KMzmtDj4_I1_rncfcU8"},
                timeout=5
            )

            data = res.json()

            print("MASSIVE DEBUG:", data)   # 🔥 ALWAYS use print for debugging

            # ❌ API error
            if "error" in data:
                return False

            # ✅ Check results properly
            results = data.get("results", [])
            return isinstance(results, list) and len(results) > 0

        except Exception as e:
            print("MASSIVE ERROR:", str(e))
            return False