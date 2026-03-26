import pandas as pd


def reconcile_data(df_y, df_a, threshold=0.02):

    # Ensure timestamp format
    df_y["timestamp"] = pd.to_datetime(df_y["timestamp"], utc=True)
    df_a["timestamp"] = pd.to_datetime(df_a["timestamp"], utc=True)

    # Use OUTER JOIN (important fix)
    df = df_y.merge(
        df_a,
        on="timestamp",
        how="outer",
        suffixes=("_y", "_a")
    )

    # Forward fill missing values
    df["adj_close_y"] = df["adj_close_y"].ffill()
    df["adj_close_a"] = df["adj_close_a"].ffill()

    # Safe price diff
    df["price_diff"] = abs(
        df["adj_close_y"] - df["adj_close_a"]
    ) / df["adj_close_y"].replace(0, 1)

    # Flags
    df["is_consistent"] = df["price_diff"] < threshold

    # Final price selection
    df["adj_close"] = df.apply(select_price, axis=1)  # ✅ IMPORTANT FIX

    # Confidence score
    df["confidence_score"] = 1 - df["price_diff"]

    # Pick OHLC (prefer yfinance)
    for col in ["open", "high", "low", "close", "volume"]:
        y_col = f"{col}_y"
        a_col = f"{col}_a"

        if y_col in df.columns:
            df[col] = df[y_col].combine_first(df.get(a_col))

    return df


def select_price(row):

    y = row.get("adj_close_y")
    a = row.get("adj_close_a")

    # Both present
    if pd.notna(y) and pd.notna(a):

        if row["is_consistent"]:
            return y

        if row["price_diff"] < 0.05:
            return (y + a) / 2

        return y

    # Only one available
    if pd.notna(y):
        return y

    if pd.notna(a):
        return a

    return None