# adapters/__init__.py

from .binance_adapter import BinanceAdapter
from .bybit_adapter import BybitAdapter
from .okx_adapter import OKXAdapter

ADAPTERS = {
    "binance": BinanceAdapter,
    "bybit": BybitAdapter,
    "okx": OKXAdapter,
}

def get_adapter(name: str):
    name = (name or "").lower()
    if name not in ADAPTERS:
        raise ValueError(f"Unknown exchange adapter: {name}")
    return ADAPTERS[name]
