from Coins import Coins
import time
from Database import Database
import config
if __name__ == "__main__":
    pair_list = ["BTCUSDT"]
    coins = Coins()
    coins.add_coin_connection(pair_list)

    while True:
        time.sleep(1)
