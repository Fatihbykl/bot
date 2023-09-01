import datetime
from enum import Enum
from dataclasses import dataclass
from datetime import datetime


class Side(str, Enum):
    BUY = 'Buy'
    SELL = 'Sell'


@dataclass
class Position:
    symbol: str
    side: Side
    size: float
    entry_price: float
    close_prices: []
    value: str
    created_time: datetime
