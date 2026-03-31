# app/normalization/normalizer.py

import pandas as pd

def normalize(df,symbol_id):

    df["symbol"] = symbol_id

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    df = df.sort_values("timestamp")
    df["symbol_id"] = symbol_id

    return df[[
        "symbol", "timestamp",
        "open", "high", "low",
        "close", "adj_close", "volume",
        "source", "ingestion_time"
    ]]