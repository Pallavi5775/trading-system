# app/features/feature_engine.py

import numpy as np

def compute_returns(df):

    df["log_return"] = np.log(df["adj_close"] / df["adj_close"].shift(1))
    df["simple_return"] = df["adj_close"].pct_change()
    df = df.ffill().bfill()

    return df


def compute_volatility(df):

    df["volatility_7d"] = df["log_return"].rolling(7).std() * np.sqrt(252)
    df["volatility_30d"] = df["log_return"].rolling(30).std() * np.sqrt(252)

    df["rolling_mean_7d"] = df["log_return"].rolling(7).mean()
    df["rolling_std_7d"] = df["log_return"].rolling(7).std()
    df = df.ffill().bfill()

    return df