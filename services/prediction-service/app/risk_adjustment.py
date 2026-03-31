def risk_adjust(pred: dict):

    vol = pred["predicted_volatility"]
    ret = pred["expected_return"]

    if vol == 0:
        score = 0
    else:
        score = ret / vol  # Sharpe-like

    # Confidence logic
    if pred["prob_up"] > 0.7:
        confidence = "HIGH"
    elif pred["prob_up"] > 0.6:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    return {
        **pred,
        "risk_adjusted_score": score,
        "confidence": confidence
    }