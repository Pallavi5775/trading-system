# services/ingestion/adapters/base.py

from abc import ABC, abstractmethod

class BaseAdapter(ABC):

    @abstractmethod
    def fetch_ohlc(self, symbol: str, start: str, end: str):
        pass

    @abstractmethod
    def fetch_fundamentals(self, symbol: str):
        pass