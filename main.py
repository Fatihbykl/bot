from Coins import Coins
import time
from Database import Database
import config
if __name__ == "__main__":
    pair_list = ["BTCUSDT", "LTCUSDT", "ASTRUSDT"]
    coins = Coins()
    coins.add_coin_connection(pair_list)
    btc = coins.get_coin_object("BTCUSDT")
    btc.calculate_rsi()
    btc.calculate_volatility()

    while True:
        time.sleep(1)
