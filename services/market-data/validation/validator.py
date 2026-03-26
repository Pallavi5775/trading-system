import numpy as np

def validate(df):

    expected_cols = ["timestamp", "open", "high", "low", "close", "adj_close", "volume"]

    missing = [col for col in expected_cols if col not in df.columns]

    if missing:
        raise ValueError(f"Missing columns before normalization: {missing}")

    # Ensure numeric columns are clean
    numeric_cols = ["open", "high", "low", "close", "adj_close", "volume"]

    df[numeric_cols] = df[numeric_cols].replace([np.inf, -np.inf], np.nan)

    # Forward fill (important for time series)
    df = df.sort_values("timestamp")
    df[numeric_cols] = df[numeric_cols].ffill().bfill()

    # Handle log_return safely
    if "log_return" not in df.columns:
        df["log_return"] = 0
    else:
        df["log_return"] = df["log_return"].replace([np.inf, -np.inf], np.nan)
        df["log_return"] = df["log_return"].fillna(0)

    # Missing flag AFTER cleaning
    df["missing_flag"] = df[numeric_cols].isnull().any(axis=1)

    # Price validation
    df["price_valid"] = (
        (df["low"] <= df["open"]) &
        (df["open"] <= df["high"]) &
        (df["low"] <= df["close"]) &
        (df["close"] <= df["high"])
    )

    # Return validation
    df["return_valid"] = df["log_return"].abs() < 0.5

    # 🔹 Quality flag
    df["quality_flag"] = "clean"

    df.loc[~df["price_valid"], "quality_flag"] = "invalid_price"
    df.loc[df["missing_flag"], "quality_flag"] = "missing"

    return df