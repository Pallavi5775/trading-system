REQUIRED_FEATURES = [
    "log_return",
    "volatility_7d",
    "rolling_std_7d",
    "momentum_intraday",
    "volume_spike",
    "overnight_return",
    "intraday_return"
]
def validate_data(raw_json):

    

    if not raw_json.get("price_valid", True):
        raise ValueError("Invalid price data")

    if raw_json.get("missing_flag", True):
        raise ValueError("Missing data")

    if raw_json.get("quality_flag") != "clean":
        raise ValueError("Data quality issue")

def extract_features(raw_json: dict):

    features = {}
    validate_data(raw_json)

    for f in REQUIRED_FEATURES:
        value = raw_json.get(f)

        if value is None:
            raise ValueError(f"Missing feature: {f}")

        features[f] = float(value)

    return features