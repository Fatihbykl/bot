from market import ManageCoins
from bot import ManageBots
import time
from db import Database
import config
import logging.config
import os
import json


def get_pair_list():
    with open(config.COIN_DATA_PATH) as f:
        pair_list = [row.split(',')[1].rstrip() for row in f]
        return pair_list


def init_coins(pair_list):
    coins = ManageCoins(intervals=config.INTERVALS, testnet=True)
    coins.update_coin_info()
    coins.add_coin_connection(pair_list)
    for coin in coins.object_dict.values():
        time.sleep(0.2)
        coin.start()
        for interval in config.INTERVALS:
            coin.calculate_indicators(interval=interval)


def init_bot():
    pass
    # bots = ManageBots(Database(config))
    # bots.start_bots()


def setup_logging(
        default_path='logging.json',
        default_level=logging.INFO,
        env_key='LOG_CFG'
):
    """ Setup logging configuration. """

    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            conf = json.load(f)
        logging.config.dictConfig(conf)
    else:
        logging.basicConfig(level=default_level)


def main():
    setup_logging()
    pair_list = get_pair_list()
    init_coins(pair_list)
    init_bot()
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
