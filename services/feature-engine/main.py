import redis
import json
import pandas as pd
from threading import Thread
from fastapi import FastAPI

app = FastAPI()

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("feature-engine")

from sqlalchemy import create_engine

DATABASE_URL = "postgresql+psycopg2://avnadmin:AVNS_SPx5mGZsHfWLTBIzEGM@pg-39985733-pallavidapriya75-97f0.h.aivencloud.com:12783/defaultdb?sslmode=require"


engine = create_engine(DATABASE_URL)



def json_serializer(obj):
    import pandas as pd
    import numpy as np

    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()

    if isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
        return None

    return obj


r = redis.Redis(
    host='redis-18652.c267.us-east-1-4.ec2.cloud.redislabs.com',
    port=18652,
    decode_responses=True,
    username="default",
    password="kZqrqVP7sC7YkkIgiNPtAs8M3NK5Gf0r",
)
from sqlalchemy import text

def store_features(features):
    try:
        logger.info(f"[DB] Storing features for {features['symbol_id']}")

        safe_features = make_json_safe(features)

        query = text("""
    INSERT INTO trading_system.intraday_features (
        symbol_id,
        timestamp,
        intradayfeature
    )
    VALUES (
        :symbol_id,
         NOW(),
        :intradayfeature
    );
""")

        payload = {
            "symbol_id": safe_features["symbol_id"],
            "intradayfeature": json.dumps(safe_features)
        }

        with engine.begin() as conn:
            conn.execute(query, payload)

        logger.info("[DB] Feature stored successfully")

    except Exception as e:
        logger.error(f"[DB ERROR] {e}", exc_info=True)


def make_json_safe(data):
    import pandas as pd
    import numpy as np

    safe = {}

    for k, v in data.items():
        if isinstance(v, pd.Timestamp):
            safe[k] = v.isoformat()

        elif isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            safe[k] = None

        else:
            safe[k] = v

    return safe


# 🔧 FEATURE COMPUTATION
def compute_features(symbol, df, window=60):

    if len(df) < 10:
        logger.warning(f"Not enough data for {symbol}")
        return

    df["hour"] = df["timestamp"].dt.hour
    df["session"] = df["hour"].apply(
        lambda x: "overnight" if x < 9 else "intraday"
    )

    df["prev_close"] = df["close"].shift(1)

    df["overnight_return"] = df["open"] / df["prev_close"] - 1
    df["intraday_return"] = df["close"] / df["open"] - 1
    df["gap"] = df["open"] / df["prev_close"] - 1
    df["momentum_intraday"] = df["close"].pct_change()

    df["volume_mean_5"] = df["volume"].rolling(5, min_periods=1).mean()
    df["volume_spike"] = df["volume"] / (df["volume_mean_5"] + 1e-9)

    df["volatility_intraday"] = df["log_return"].rolling(5, min_periods=1).std()

    feature_cols = [
        "overnight_return",
        "intraday_return",
        "gap",
        "momentum_intraday",
        "volume_spike",
        "volatility_intraday"
    ]

    df[feature_cols] = df[feature_cols].shift(1)

    # remove first row (shift artifact)
    df = df.iloc[1:]

    df = df.replace([float("inf"), float("-inf")], None)

    df_valid = df.dropna(subset=feature_cols)

    if df_valid.empty:
        logger.warning("No valid features after processing")
        return

    for _, row in df_valid.iterrows():
        store_features({
            "symbol_id": symbol,
            "timestamp": row["timestamp"],
            "intradayfeature": {
                col: row[col] for col in feature_cols
            }
        })

    logger.info(f"Stored {len(df_valid)} rows")


# if gap > 0 and momentum_intraday > 0:
#     signal = "BUY"

# elif gap < 0 and momentum_intraday < 0:
#     signal = "SELL"

# else:
#    


def load_from_db(symbol, limit=60):
    try:
        from sqlalchemy.orm import Session

        logger.info(f"[DB] Fetching history for {symbol}")

        df = pd.read_sql(
            f"""
            SELECT *
            FROM trading_system.market_data
            WHERE symbol_id = '{symbol}'
            ORDER BY timestamp DESC
            LIMIT {limit}
            """,
            engine
        )

        if df.empty:
            logger.warning(f"[DB] No data found for {symbol}")
            return None

        df = df.sort_values("timestamp")

        logger.info(f"[DB] Loaded {len(df)} rows for {symbol}")
        return df

    except Exception as e:
        logger.error(f"[DB ERROR] {e}")
        return None
    
def load_from_redis(symbol, limit=10):
    key = f"market_data:{symbol}"

    logger.info(f"[REDIS] Fallback loading for {symbol}")

    history = r.lrange(key, 0, limit - 1)

    if not history:
        logger.warning(f"[REDIS] No fallback data for {symbol}")
        return None

    history = [json.loads(x) for x in history]

    df = pd.DataFrame(history)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")

    logger.info(f"[REDIS] Loaded {len(df)} fallback rows")





@app.post("/compute_features/{symbol}")
def get_features(symbol: str):
    logger.info(f"[START] compute_features for {symbol}")

    # --- Try DB first ---
    df = load_from_db(symbol, limit=60)

    # --- Fallback to Redis ---
    if df is None or len(df) < 5:
        logger.warning(f"[FALLBACK] Using Redis for {symbol}")
        df = load_from_redis(symbol, limit=10)

    # --- If still nothing ---
    if df is None or df.empty:
        logger.error(f"[FAIL] No data available for {symbol}")
        return {}

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")

    logger.info(f"[DATA] Final dataset size: {len(df)}")

    if df.empty:
        logger.warning(f"[EMPTY DF] No data for {symbol}")
        return {}

    # Convert timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")

    logger.info(f"[DATA] Time range: {df['timestamp'].min()} → {df['timestamp'].max()}")

    compute_features(symbol, df)

    return True
    
    
    

@app.get("/features/{symbol}")
def get_features(symbol: str):
    data = json.loads(r.get(f"features:{symbol}") or "{}")

    # Final cleanup before response
    for k, v in data.items():
        if isinstance(v, float) and (v != v or v == float("inf") or v == float("-inf")):
            data[k] = None

    return data