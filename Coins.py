from CoinData import CoinData
import time


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

        for pair in pair_names:
            if not self.object_dict.get(pair):
                obj = CoinData(symbol=pair, channel_type='linear', testnet=False, interval='1')
                obj.start()
                self.object_dict[pair] = obj
                time.sleep(0.5)

    def get_coin_object(self, pair_name):
        """Get object for given pair_name"""

        return self.object_dict.get(pair_name)
