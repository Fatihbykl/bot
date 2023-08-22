import datetime

from CoinData import CoinData
from pybit.unified_trading import MarketHTTP
import time
from Database import Database
import config

# 1683849600000 timestamp
class Coins:
    """CoinData initiator class"""

    instance = None
    object_dict = {}

    def __new__(cls):
        """Code for Singleton"""

        if not isinstance(cls.instance, cls):
            cls.instance = super(Coins, cls).__new__(cls)
        return cls.instance

    def add_coin_connection(self, pair_names):
        """Create objects and start websocket connections."""
        start_time = time.time()
        session = MarketHTTP(testnet=False)
        db = Database(config)

        for pair in pair_names:
            if not self.object_dict.get(pair):
                obj = CoinData(symbol=pair, channel_type='linear', testnet=False)
                obj.start()
                self.object_dict[pair] = obj

                intervals = [15, 60, 240, 'D']
                for interval in intervals:
                    time.sleep(0.1)
                    tuple_list = []
                    data = session.get_kline(category='linear', symbol=pair, interval=interval, limit=100)
                    for item in data['result']['list']:
                        timestamp = datetime.datetime.fromtimestamp(int(item[0]) / 1000)
                        open = item[1]
                        high = item[2]
                        low = item[3]
                        close = item[4]
                        volume = item[5]
                        tuple_list.append((pair, timestamp, open, close, high, low, volume, interval))
                    db.insert_multiple_row_kline(tuple_list)
                time.sleep(0.5)

            end_time = time.time()
            print(end_time - start_time)

    def get_coin_object(self, pair_name):
        """Get object for given pair_name"""

        return self.object_dict.get(pair_name)
