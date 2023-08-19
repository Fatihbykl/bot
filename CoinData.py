import numpy
import numpy as np
from pybit.unified_trading import WebSocket
from time import sleep
from talib import _ta_lib as talib
from Database import Database
import config
from datetime import datetime


class CoinData:
    """Class for each pair data"""

    def __init__(
            self,
            symbol=None,
            channel_type=None,
            testnet=None,
            interval=None
    ):
        self.symbol = symbol
        self.channel_type = channel_type
        self.testnet = testnet
        self.interval = interval
        self.db = Database(config)

    def start(self):
        """Start websocket connection"""

        ws = WebSocket(testnet=self.testnet, channel_type=self.channel_type)
        ws.kline_stream(self.interval, self.symbol, self.handle_data)

    def handle_data(self, data):
        """Get data from websocket and save candle closes to database"""

        print(data)
        _topic = str(data['topic']).split('.')[-1]
        data = data['data'][0]
        _timestamp = datetime.fromtimestamp(data['start']/1000)
        _interval = data['interval']
        _open = data['open']
        _close = data['close']
        _high = data['high']
        _low = data['low']
        _volume = data['volume']

        is_closed = data["confirm"]
        if is_closed:
            self.db.insert_row(_topic=_topic, _timestamp=_timestamp, _interval=_interval, _open=_open, _close=_close,
                               _high=_high, _low=_low, _volume=_volume)
            # save to database
            # calculate things(rsi, cart curt) again

    def calculate_rsi(self):
        """Calculate RSI"""

        ohlc = self.db.get_last_ohlc(self.symbol)
        rsi = talib.RSI(ohlc['close'], 14)
        print(rsi)

    def calculate_volatility(self):
        """Calculate NATR"""

        ohlc = self.db.get_last_ohlc(self.symbol)
        natr = talib.NATR(ohlc['high'], ohlc['low'], ohlc['close'])
        print(natr)


