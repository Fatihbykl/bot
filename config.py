from os import environ, path
from dotenv import load_dotenv
import sys

basedir = path.abspath(path.dirname(__file__))
sys.path.append(basedir)

load_dotenv(path.join(basedir, '.env'))

CSV_PATH = path.join(basedir, 'csv_data')
CMC_DATA_PATH = path.join(CSV_PATH, 'cmc_data.csv')
COIN_DATA_PATH = path.join(CSV_PATH, 'coin_data.csv')

#INTERVALS = [1, 15, 60, 240, 'D']
INTERVALS = [1]
DATABASE_HOST = environ.get('DATABASE_HOST')
DATABASE_USERNAME = environ.get('DATABASE_USERNAME')
DATABASE_PASSWORD = environ.get('DATABASE_PASSWORD')
DATABASE_PORT = environ.get('DATABASE_PORT')
DATABASE_NAME = environ.get('DATABASE_NAME')

BYBIT_API_KEY = environ.get('BYBIT_API_KEY')
BYBIT_SECRET = environ.get('BYBIT_SECRET')

CMC_API_KEY = environ.get('CMC_API_KEY')

SQL_QUERIES_FOLDER = 'sql'
