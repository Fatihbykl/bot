import datetime
from enum import Enum
from dataclasses import dataclass
from datetime import datetime


class Side(str, Enum):
    BUY = 'Buy'
    SELL = 'Sell'
    CLOSE = 'Close'
    NO_SIGNAL = 'No Signal'


@dataclass
class Position:
    symbol: str
    side: Side
    size: str
    entry_price: str
    value: str
    created_time: str

@dataclass
class StrategyInfo:
    r_value: str
    strategy: object
    sl_buy: str
    sl_sell: str
    tp_buy: str
    tp_sell: str
    

@dataclass
class TradeData:
    current_price: str
    candle_closed: bool
    ohlc_data: {}
    signal: Side

@dataclass
class PaperTrade:
    symbol: str
    side: str
    realized_pl: str
    entry_price: str
    closed_prices: []
    sl_price: str
    tp_price: str
    created_time: str
