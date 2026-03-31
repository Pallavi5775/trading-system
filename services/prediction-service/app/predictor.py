import numpy as np
from app.model_loader import model_loader

MODEL_VERSION = "v2.0-multi-model"


def predict(features: dict):

    loader = model_loader.load()

    # -------------------------------
    # PREPARE INPUT
    # -------------------------------
    try:
        X = np.array([[features[f] for f in loader.features]])
    except KeyError as e:
        raise ValueError(f"Missing feature: {e}")

    # -------------------------------
    # MODEL OUTPUTS
    # -------------------------------
    prob_up = float(loader.clf.predict_proba(X)[0][1])
    expected_return = float(loader.reg.predict(X)[0])
    predicted_volatility = float(loader.vol.predict(X)[0])

    return {
        "prob_up": prob_up,
        "expected_return": expected_return,
        "predicted_volatility": predicted_volatility,
        "model_version": MODEL_VERSION
    }