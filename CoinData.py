import time

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
    ):
        self.symbol = symbol
        self.channel_type = channel_type
        self.testnet = testnet
        self.db = Database(config)
        self.current_price = 0

    def start(self):
        """Start websocket connection"""

        ws = WebSocket(testnet=self.testnet, channel_type=self.channel_type)
        ws.kline_stream(15, self.symbol, self.handle_data)
        ws.kline_stream(60, self.symbol, self.handle_data)
        ws.kline_stream(240, self.symbol, self.handle_data)
        ws.kline_stream('D', self.symbol, self.handle_data)

    def handle_data(self, data):
        """Get data from websocket and save candle closes to database"""

        print(data)
        _topic = str(data['topic']).split('.')[-1]
        data = data['data'][0]
        _timestamp = datetime.fromtimestamp(data['start'] / 1000)
        _interval = data['interval']
        _open = data['open']
        _close = data['close']
        _high = data['high']
        _low = data['low']
        _volume = data['volume']

        self.current_price = _close

        is_closed = data["confirm"]
        if is_closed:
            self.db.insert_row_kline(_topic=_topic, _timestamp=_timestamp, _interval=_interval, _open=_open,
                                     _close=_close, _high=_high, _low=_low, _volume=_volume)
            if _interval == '15':
                self.calculate_indicators('15')
            elif _interval == '60':
                self.calculate_indicators('60')
            elif _interval == '240':
                self.calculate_indicators('240')
            elif _interval == 'D':
                self.calculate_indicators('D')

    def calculate_indicators(self, interval):
        """Calculate indicators"""

        ohlc = self.db.get_last_ohlc(self.symbol, interval)

        rsi = talib.RSI(ohlc['close'], 14)
        natr = talib.NATR(ohlc['high'], ohlc['low'], ohlc['close'])

        self.db.insert_row_coindata(self.symbol, interval, rsi[-1], natr[-1], '0')


