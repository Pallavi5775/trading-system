from sqlalchemy import create_engine, text
import pandas as pd

DATABASE_URL = "postgresql+psycopg2://avnadmin:AVNS_SPx5mGZsHfWLTBIzEGM@pg-39985733-pallavidapriya75-97f0.h.aivencloud.com:12783/defaultdb?sslmode=require"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


# ✅ CENTRAL FEATURE LIST (VERY IMPORTANT)
FEATURE_COLUMNS = [
    "gap",
    "volume_spike",
    "intraday_return",
    "overnight_return",
    "momentum_intraday",
    "volatility_intraday"
]


# -----------------------------------
# FETCH TRAINING DATA (OPTIMIZED)
# -----------------------------------
def fetch_training_data(limit=50):

    query = text("""
        SELECT 
            symbol_id,
            timestamp,
            intradayfeature
        FROM trading_system.intraday_features
        ORDER BY timestamp ASC
        LIMIT :limit
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"limit": limit})

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # ✅ Extract inner JSON safely
    df["intradayfeature"] = df["intradayfeature"].apply(
        lambda x: x.get("intradayfeature", {}) if isinstance(x, dict) else {}
    )

    # ✅ Flatten JSON
    features_df = pd.json_normalize(df["intradayfeature"])

    # ✅ Combine metadata + features
    final_df = pd.concat(
        [df[["symbol_id", "timestamp"]].reset_index(drop=True), features_df],
        axis=1
    )

    print(final_df.head())

    return final_df


def clean_data(df):

    # 1. Sort (VERY IMPORTANT for time series)
    df = df.sort_values("timestamp")

    # 2. Drop rows where target is NaN (last rows)
    df = df.dropna(subset=["target_return", "target_vol"])

    # 3. Forward fill features (safe for time series)
    df[FEATURE_COLUMNS] = df[FEATURE_COLUMNS].ffill()

    # 4. Drop any remaining NaNs
    df = df.dropna(subset=FEATURE_COLUMNS)

    # 5. Remove duplicates
    df = df.drop_duplicates(subset=["symbol_id", "timestamp"])

    return df


# -----------------------------------
# FETCH LATEST (FOR PREDICTION)
# -----------------------------------
def fetch_latest(symbol_id):

    query = text(f"""
        SELECT 
            symbol_id,
            timestamp,

            (intradayfeature->>'gap')::float AS gap,
            (intradayfeature->>'volume_spike')::float AS volume_spike,
            (intradayfeature->>'intraday_return')::float AS intraday_return,
            (intradayfeature->>'overnight_return')::float AS overnight_return,
            (intradayfeature->>'momentum_intraday')::float AS momentum_intraday,
            (intradayfeature->>'volatility_intraday')::float AS volatility_intraday

        FROM trading_system.intraday_features
        WHERE symbol_id = :symbol_id
        ORDER BY timestamp DESC
        LIMIT 1
    """)

    with engine.connect() as conn:
        row = conn.execute(query, {"symbol_id": symbol_id}).fetchone()

    if not row:
        raise ValueError(f"No data found for {symbol_id}")

    # Convert to dict
    result = dict(row._mapping)

    return result