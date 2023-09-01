import time

import numpy
import numpy as np
from pybit.unified_trading import WebSocket
from pybit.unified_trading import MarketHTTP
from time import sleep
from talib import _ta_lib as talib
from db import Database
import config
from datetime import datetime


class ManageCoins:
    """ CoinData initiator class. """

    instance = None
    object_dict = {}

    def __new__(cls):
        """ Code for Singleton. """

        if not isinstance(cls.instance, cls):
            cls.instance = super(ManageCoins, cls).__new__(cls)
        return cls.instance

    def add_coin_connection(self, pair_names):
        """ Create objects and start websocket connections. """

        start_time = time.time()
        session = MarketHTTP(testnet=False)
        db = Database(config)
        intervals = [1, 15, 60, 240, 'D']

        for pair in pair_names:
            if not self.object_dict.get(pair):
                obj = Coin(symbol=pair, channel_type='linear', testnet=False)
                obj.start()
                self.object_dict[pair] = obj

                for interval in intervals:
                    time.sleep(0.1)

                    values = self.extract_values(session, pair, interval)

                    db.insert_row_coindata(
                        topic=pair + '_' + str(interval),
                        interval=interval,
                        rsi=0,
                        natr=0,
                        volume='0')
                    db.insert_multiple_row_kline(values=values)
                    obj.calculate_indicators(interval)
                time.sleep(0.5)

            end_time = time.time()
            print(end_time - start_time)

    @staticmethod
    def extract_values(session, pair, interval):
        values = []
        data = session.get_kline(category='linear', symbol=pair, interval=interval, limit=100)
        for item in data['result']['list']:
            timestamp = datetime.fromtimestamp(int(item[0]) / 1000)
            open = item[1]
            high = item[2]
            low = item[3]
            close = item[4]
            volume = item[5]
            values.append((pair, timestamp, open, close, high, low, volume, interval))
        return values

    def get_coin_object(self, pair_name):
        """ Get object for given pair_name. """

        return self.object_dict.get(pair_name)


class Coin:
    """ Class for each pair data. """

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
        """ Start websocket connection. """

        ws = WebSocket(testnet=self.testnet, channel_type=self.channel_type)
        ws.kline_stream(1, self.symbol, self.handle_data)
        ws.kline_stream(15, self.symbol, self.handle_data)
        ws.kline_stream(60, self.symbol, self.handle_data)
        ws.kline_stream(240, self.symbol, self.handle_data)
        ws.kline_stream('D', self.symbol, self.handle_data)

    def handle_data(self, data):
        """ Get data from websocket and save candle closes to database. """

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
        self.current_price = (_close, is_closed)

    def calculate_indicators(self, interval):
        """ Calculate indicators. """

        ohlc = self.db.get_last_ohlc(self.symbol, interval)

        rsi = talib.RSI(ohlc['close'], 14)
        natr = talib.NATR(ohlc['high'], ohlc['low'], ohlc['close'])

        pair_name = self.symbol + '_' + str(interval)
        self.db.update_row_coindata(pair_name, rsi[-1], natr[-1], '0')


