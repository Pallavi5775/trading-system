import joblib
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import accuracy_score, mean_squared_error
import pandas as pd
from training.db import fetch_training_data, clean_data
import numpy as np

# -------------------------------
# FEATURES
# -------------------------------
REQUIRED_FEATURES = [
    "overnight_return",
    "intraday_return",
    "gap",
    "momentum_intraday",
    "volume_spike",
    "volatility_intraday"
]

# -------------------------------
# TARGET ENGINEERING
# -------------------------------
def create_targets(df):
    df["target_return"] = df["intraday_return"].shift(-1)
    df["target_class"] = (df["target_return"] > 0).astype(int)
    df["target_vol"] = df["intraday_return"].rolling(5).std().shift(-1)
    return df

# -------------------------------
# LOAD DATA
# -------------------------------
df = fetch_training_data(limit=200)

# Sort first (important)
df = df.sort_values("timestamp")

# Create targets
df = create_targets(df)

# 🔵 Extract latest row BEFORE cleaning
latest_row = df.iloc[-1]

# -------------------------------
# CLEAN TRAINING DATA
# -------------------------------
df = df.dropna(subset=["target_return", "target_vol"])
df = clean_data(df)

# -------------------------------
# TIME SPLIT (NO LEAKAGE)
# -------------------------------
split_idx = int(len(df) * 0.8)

train = df.iloc[:split_idx]
test = df.iloc[split_idx:]

X_train = train[REQUIRED_FEATURES]
X_test = test[REQUIRED_FEATURES]

y_train_class = train["target_class"]
y_test_class = test["target_class"]

y_train_return = train["target_return"]
y_test_return = test["target_return"]

y_train_vol = train["target_vol"]
y_test_vol = test["target_vol"]

# -------------------------------
# MODELS
# -------------------------------
clf = RandomForestClassifier(n_estimators=200, max_depth=6, random_state=42)
reg = RandomForestRegressor(n_estimators=200, max_depth=6, random_state=42)
vol = RandomForestRegressor(n_estimators=200, max_depth=6, random_state=42)

# -------------------------------
# TRAIN
# -------------------------------
clf.fit(X_train, y_train_class)
reg.fit(X_train, y_train_return)
vol.fit(X_train, y_train_vol)

# -------------------------------
# EVALUATION
# -------------------------------
print("Classification Accuracy:",
      accuracy_score(y_test_class, clf.predict(X_test)))

print("Return RMSE:",
      mean_squared_error(y_test_return, reg.predict(X_test)) ** 0.5)

print("Volatility RMSE:",
      mean_squared_error(y_test_vol, vol.predict(X_test)) ** 0.5)

# -------------------------------
# PREDICT LATEST (LIVE)
# -------------------------------
X_latest = pd.DataFrame([latest_row[REQUIRED_FEATURES]])

print("Latest Prediction:")
print("Class:", clf.predict(X_latest)[0])
print("Return:", reg.predict(X_latest)[0])
print("Volatility:", vol.predict(X_latest)[0])

# -------------------------------
# SAVE
# -------------------------------
joblib.dump(clf, "models/classifier.pkl")
joblib.dump(reg, "models/regressor.pkl")
joblib.dump(vol, "models/volatility.pkl")
joblib.dump(REQUIRED_FEATURES, "models/features.pkl")

print("Models trained and saved")
print(df["target_class"].value_counts(normalize=True))



df_test = test.copy()

pred_return = reg.predict(X_test)
prob = clf.predict_proba(X_test)[:, 1]

# -------------------------------
# SIGNAL GENERATION
# -------------------------------
df_test["signal"] = 0

df_test.loc[
    (prob > 0.6) & (pred_return > 0.002),
    "signal"
] = 1

df_test.loc[
    (prob < 0.4) & (pred_return < -0.002),
    "signal"
] = -1

# -------------------------------
# POSITION SIZING (SAFER)
# -------------------------------
df_test["position_size"] = np.clip(
    abs(pred_return) / 0.02,   # 🔥 less aggressive scaling
    0,
    1
)

df_test["position"] = df_test["signal"] * df_test["position_size"]

# -------------------------------
# TRANSACTION COSTS (FIXED)
# -------------------------------
cost_per_trade = 0.0005  # 5 bps

df_test["trade_change"] = df_test["position"].diff().abs()
df_test["trade_change"] = df_test["trade_change"].fillna(0)  # ✅ FIX

df_test["cost"] = df_test["trade_change"] * cost_per_trade

# -------------------------------
# STRATEGY RETURNS
# -------------------------------
df_test["strategy_return"] = (
    df_test["position"] * df_test["target_return"]
    - df_test["cost"]
)

# -------------------------------
# EQUITY CURVE
# -------------------------------
df_test["equity"] = (1 + df_test["strategy_return"]).cumprod()

# -------------------------------
# DRAWDOWN
# -------------------------------
df_test["peak"] = df_test["equity"].cummax()
df_test["drawdown"] = df_test["equity"] / df_test["peak"] - 1

# -------------------------------
# METRICS (ROBUST)
# -------------------------------
total_return = df_test["equity"].iloc[-1] - 1

mean_ret = df_test["strategy_return"].mean()
std_ret = df_test["strategy_return"].std()

sharpe = mean_ret / std_ret if std_ret != 0 else 0  # ✅ FIX

max_dd = df_test["drawdown"].min()

win_rate = (df_test["strategy_return"] > 0).mean()

# -------------------------------
# PRINT RESULTS
# -------------------------------
print("\n===== STRATEGY PERFORMANCE =====")
print(f"Total Return: {total_return:.4f}")
print(f"Sharpe Ratio: {sharpe:.4f}")
print(f"Max Drawdown: {max_dd:.4f}")
print(f"Win Rate: {win_rate:.4f}")
print(f"Avg Return per Step: {mean_ret:.6f}")

# -------------------------------
# SANITY CHECKS
# -------------------------------
num_trades = (df_test["trade_change"] > 0).sum()
print(f"Number of Trades: {num_trades}")

if num_trades == 0:
    print("⚠️ WARNING: No trades executed → thresholds too strict")

# https://chatgpt.com/g/g-p-69ca851bc9488191b447cd9940366309-project-1/c/69cabf40-e3c8-8324-b633-083c650dae55    