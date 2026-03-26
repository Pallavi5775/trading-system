# app/ingestion/adapters/alpha_vantage_adapter.py

import requests
import pandas as pd
from datetime import datetime

ALPHA_API_KEY = "PIO88D64WECKACN2"
class AlphaVantageAdapter:

    def fetch_ohlc(self, symbol):
        url = f"https://www.alphavantage.co/query"
        params = {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": symbol,
            "apikey": ALPHA_API_KEY
        }

        response = requests.get(url, params=params).json()

        data = response.get("Time Series (Daily)", {})

        print(f"alphavantage --------> {data.items()}")

        rows = []
        for date, values in data.items():
            rows.append({
                "timestamp": date,
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "adj_close": float(values["5. adjusted close"]),
                "volume": float(values["6. volume"]),
                "source": "alpha_vantage",
                "ingestion_time": datetime.utcnow()
            })

        return pd.DataFrame(rows)