# app/utils/symbol_mapper.py

def get_alpha_symbol(base_symbol: str, country: str):
    """
    Convert internal/base symbol → Alpha Vantage symbol
    """

    if country == "US":
        return base_symbol

    elif country == "IN":
        # Alpha uses BSE, not NSE
        return f"{base_symbol}.BSE"

    elif country == "UK":
        return f"{base_symbol}.LON"

    elif country == "CA":
        return f"{base_symbol}.TRT"

    elif country == "DE":
        return f"{base_symbol}.DEX"

    else:
        return None
    
    