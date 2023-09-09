import csv
import config
import logging
from coinmarketcapapi import CoinMarketCapAPI


def get_coin_id_from_file(file_name):
    with open(file_name, 'r') as file:
        lines = file.read().splitlines()
    return lines


def write_coins_to_file(data, filename):
    with open(filename, 'w', newline='') as file:
        csvwriter = csv.writer(file)
        csvwriter.writerows(data)


def get_data(id_list, api_key, limit):
    cmc = CoinMarketCapAPI(api_key=api_key)
    coins = cmc.cryptocurrency_listings_latest(limit=limit)

    coin_data = []
    for coin in coins.data:
        if str(coin['id']) in id_list:
            """
            d = {
                'id': coin['id'],
                'name': coin['name'],
                'symbol': coin['symbol'],
                'circulating_supply': int(coin['circulating_supply']),
                'volume_24h': int(coin['quote']['USD']['volume_24h']),
                'market_cap': int(coin['quote']['USD']['market_cap'])
            }"""
            coin_data.append([
                coin['id'],
                coin['name'],
                coin['symbol'],
                coin['circulating_supply'],
                coin['quote']['USD']['volume_24h'],
                coin['quote']['USD']['market_cap']
            ])
            # coin_data.append(d)
    return coin_data


def create_csv(ids_path, data_path, limit):
    logger = logging.getLogger()
    try:
        ids = get_coin_id_from_file(ids_path)
        coin_data = get_data(id_list=ids, api_key=config.CMC_API_KEY, limit=limit)
        write_coins_to_file(data=coin_data, filename=data_path)
    except Exception as e:
        logger.exception(e)
        raise e
