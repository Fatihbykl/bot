import csv
import decimal
import config
import logging
import pandas as pd
from coinmarketcapapi import CoinMarketCapAPI


def write_coins_to_file(data, filename):
    with open(filename, 'w', newline='') as file:
        csvwriter = csv.writer(file)
        csvwriter.writerows(data)


def get_data(id_list, api_key, limit):
    cmc = CoinMarketCapAPI(api_key=api_key)
    coins = cmc.cryptocurrency_listings_latest(limit=limit)
    coin_data = []
    for coin in coins.data:
        if coin['id'] in id_list:
            volume = coin['quote']['USD']['volume_24h']
            market_cap = coin['quote']['USD']['market_cap']
            vol_mcap = decimal.Decimal(volume) / decimal.Decimal(market_cap)
            vol_mcap = round(vol_mcap * 100, 2)
            coin_data.append([
                coin['id'],
                coin['name'],
                coin['symbol'],
                coin['circulating_supply'],
                volume,
                market_cap,
                vol_mcap
            ])
    return coin_data


def create_csv(api_coin_limit):
    logger = logging.getLogger()
    try:
        ids = pd.read_csv(config.COIN_DATA_PATH)['id'].values
        coin_data = get_data(id_list=ids, api_key=config.CMC_API_KEY, limit=api_coin_limit)
        write_coins_to_file(data=coin_data, filename=config.CMC_DATA_PATH)
    except Exception as e:
        logger.exception(e)
        raise e
