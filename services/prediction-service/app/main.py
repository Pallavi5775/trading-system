from fastapi import FastAPI
from training.db import fetch_latest
from training.feature_extractor import extract_features, validate_data
from app.predictor import predict
from app.risk_adjustment import risk_adjust
app = FastAPI()

def run_prediction(symbol_id: str):

    raw = fetch_latest(symbol_id)

    # VALIDATION
    validate_data(raw)

    # FEATURE EXTRACTION
    features = extract_features(raw)

    # MODEL
    pred = predict(features)

    # RISK ADJUSTMENT
    final = risk_adjust(pred)

    return {
        "symbol_id": symbol_id,
        "timestamp": raw["timestamp"],
        **final
    }



@app.get("/predict/{symbol_id}")
def predict_api(symbol_id: str):
    return run_prediction(symbol_id)