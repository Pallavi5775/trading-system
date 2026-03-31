from database import SessionLocal
from models import SymbolRegistry, SymbolSourceMapping


def load_symbols():

    print("SYMBOL LOADER STARTED")

    db = SessionLocal()

    try:
        symbols = [

        # 🇺🇸 US EQUITIES
        {
            "symbol_id": "EQ_US_AAPL",
            "base_symbol": "AAPL",
            "exchange": "NASDAQ",
            "country": "US",
            "currency": "USD",
            "timezone": "America/New_York",
            "sector": "technology",
            "yfinance": "AAPL",
            "alpha": "AAPL"
        },
        {
            "symbol_id": "EQ_US_MSFT",
            "base_symbol": "MSFT",
            "exchange": "NASDAQ",
            "country": "US",
            "currency": "USD",
            "timezone": "America/New_York",
            "sector": "technology",
            "yfinance": "MSFT",
            "alpha": "MSFT"
        },
        {
            "symbol_id": "EQ_US_AMZN",
            "base_symbol": "AMZN",
            "exchange": "NASDAQ",
            "country": "US",
            "currency": "USD",
            "timezone": "America/New_York",
            "sector": "consumer",
            "yfinance": "AMZN",
            "alpha": "AMZN"
        },
        {
            "symbol_id": "EQ_US_NVDA",
            "base_symbol": "NVDA",
            "exchange": "NASDAQ",
            "country": "US",
            "currency": "USD",
            "timezone": "America/New_York",
            "sector": "technology",
            "yfinance": "NVDA",
            "alpha": "NVDA"
        },
        {
            "symbol_id": "EQ_US_JPM",
            "base_symbol": "JPM",
            "exchange": "NYSE",
            "country": "US",
            "currency": "USD",
            "timezone": "America/New_York",
            "sector": "financials",
            "yfinance": "JPM",
            "alpha": "JPM"
        },

        # 🇮🇳 INDIA EQUITIES (🔥 FIXED)
        {
            "symbol_id": "EQ_IN_RELIANCE",
            "base_symbol": "RELIANCE",
            "exchange": "NSE",
            "country": "IN",
            "currency": "INR",
            "timezone": "Asia/Kolkata",
            "sector": "energy",
            "yfinance": "RELIANCE.NS",
            "alpha": "RELIANCE.BSE"   # 🔥 FIX
        },
        {
            "symbol_id": "EQ_IN_TCS",
            "base_symbol": "TCS",
            "exchange": "NSE",
            "country": "IN",
            "currency": "INR",
            "timezone": "Asia/Kolkata",
            "sector": "technology",
            "yfinance": "TCS.NS",
            "alpha": "TCS.BSE"        # 🔥 FIX
        },
        {
            "symbol_id": "EQ_IN_HDFCBANK",
            "base_symbol": "HDFCBANK",
            "exchange": "NSE",
            "country": "IN",
            "currency": "INR",
            "timezone": "Asia/Kolkata",
            "sector": "financials",
            "yfinance": "HDFCBANK.NS",
            "alpha": "HDFCBANK.BSE"   # 🔥 FIX
        },

        # 📊 ETFs
        {
            "symbol_id": "ETF_US_SPY",
            "base_symbol": "SPY",
            "exchange": "NYSE",
            "country": "US",
            "currency": "USD",
            "timezone": "America/New_York",
            "sector": "index",
            "yfinance": "SPY",
            "alpha": "SPY"
        },
        {
            "symbol_id": "ETF_US_QQQ",
            "base_symbol": "QQQ",
            "exchange": "NASDAQ",
            "country": "US",
            "currency": "USD",
            "timezone": "America/New_York",
            "sector": "technology",
            "yfinance": "QQQ",
            "alpha": "QQQ"
        },

        # 📉 INDICES (Alpha does NOT support)
        {
            "symbol_id": "IDX_US_SP500",
            "base_symbol": "SP500",
            "exchange": "INDEX",
            "country": "US",
            "currency": "USD",
            "timezone": "America/New_York",
            "sector": "index",
            "yfinance": "^GSPC",
            "alpha": None   # ❌ keep None
        },
        {
            "symbol_id": "IDX_IN_NIFTY50",
            "base_symbol": "NIFTY50",
            "exchange": "INDEX",
            "country": "IN",
            "currency": "INR",
            "timezone": "Asia/Kolkata",
            "sector": "index",
            "yfinance": "^NSEI",
            "alpha": None   # ❌ keep None
        },

        # 🌍 GLOBAL EXAMPLES (NEW)

        # 🇬🇧 UK
        {
            "symbol_id": "EQ_UK_TSCO",
            "base_symbol": "TSCO",
            "exchange": "LSE",
            "country": "UK",
            "currency": "GBX",
            "timezone": "Europe/London",
            "sector": "consumer",
            "yfinance": "TSCO.L",
            "alpha": "TSCO.LON"
        },

        # 🇨🇦 Canada
        {
            "symbol_id": "EQ_CA_SHOP",
            "base_symbol": "SHOP",
            "exchange": "TSX",
            "country": "CA",
            "currency": "CAD",
            "timezone": "America/Toronto",
            "sector": "technology",
            "yfinance": "SHOP.TO",
            "alpha": "SHOP.TRT"
        },

        # 🇩🇪 Germany
        {
            "symbol_id": "EQ_DE_MBG",
            "base_symbol": "MBG",
            "exchange": "XETRA",
            "country": "DE",
            "currency": "EUR",
            "timezone": "Europe/Berlin",
            "sector": "automobile",
            "yfinance": "MBG.DE",
            "alpha": "MBG.DEX"
        }
    ]

        for s in symbols:

            print(f"Processing {s['symbol_id']}")

            # Check if exists
            existing = db.query(SymbolRegistry).filter_by(symbol_id=s["symbol_id"]).first()

            if existing:
                print("Already exists, skipping")
                continue

            # Insert registry
            reg = SymbolRegistry(
                symbol_id=s["symbol_id"],
                base_symbol=s["base_symbol"],
                exchange=s["exchange"],
                country=s["country"],
                currency=s["currency"],
                timezone=s["timezone"],
                sector=s["sector"]
            )

            db.add(reg)

            # Insert mapping
            mapping = SymbolSourceMapping(
                symbol_id=s["symbol_id"],
                yfinance_symbol=s["yfinance"],
                alpha_vantage_symbol=s["alpha"]
            )

            db.add(mapping)

        db.commit()
        print("COMMIT SUCCESSFUL")

    except Exception as e:
        print("ERROR:", e)
        db.rollback()

    finally:
        db.close()


if __name__ == "__main__":
    load_symbols()