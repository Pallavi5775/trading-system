import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

class YFinanceAdapter:

    def fetch_ohlc(self, symbol, start=None, end=None):

        end = datetime.utcnow()
        start = end - timedelta(days=60)

        df = yf.download(symbol, start=start, end=end)

        df = df.reset_index()

        # Handle MultiIndex HERE (not globally)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0].lower() for col in df.columns]

        else:
            df.columns = [col.lower() for col in df.columns]

        # Standardize timestamp
        if "date" in df.columns:
            df.rename(columns={"date": "timestamp"}, inplace=True)

        # Ensure required columns
        required = ["timestamp", "open", "high", "low", "close", "volume"]
        for col in required:
            if col not in df.columns:
                raise ValueError(f"YFinance missing column: {col}")

        # adj_close fallback
        if "adj close" in df.columns:
            df.rename(columns={"adj close": "adj_close"}, inplace=True)
        else:
            df["adj_close"] = df["close"]

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        df["source"] = "yfinance"
        df["ingestion_time"] = datetime.utcnow()

        return df