# app/normalization/normalizer.py

import pandas as pd

def normalize(df, symbol):

    df["symbol"] = symbol

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    df = df.sort_values("timestamp")

    return df[[
        "symbol", "timestamp",
        "open", "high", "low",
        "close", "adj_close", "volume",
        "source", "ingestion_time"
    ]]