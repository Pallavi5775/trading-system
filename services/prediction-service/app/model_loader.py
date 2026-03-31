import joblib

class ModelLoader:
    def __init__(self):
        self.clf = None
        self.reg = None
        self.vol = None
        self.features = None

    def load(self):
        if self.clf is None:
            self.clf = joblib.load("models/classifier.pkl")

        if self.reg is None:
            self.reg = joblib.load("models/regressor.pkl")

        if self.vol is None:
            self.vol = joblib.load("models/volatility.pkl")

        if self.features is None:
            self.features = joblib.load("models/features.pkl")

        return self

model_loader = ModelLoader()