from market import ManageCoins
from bot import ManageBots
import time
from db import Database
import config

if __name__ == "__main__":
    pair_list = ["ASTRUSDT"]
    coins = ManageCoins()
    coins.add_coin_connection(pair_list)
    bots = ManageBots(Database(config))
    bots.start_bots()

    while True:
        time.sleep(1)
