from os import environ, path
from dotenv import load_dotenv

basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, '.env'))

DATABASE_HOST = environ.get('DATABASE_HOST')
DATABASE_USERNAME = environ.get('DATABASE_USERNAME')
DATABASE_PASSWORD = environ.get('DATABASE_PASSWORD')
DATABASE_PORT = environ.get('DATABASE_PORT')
DATABASE_NAME = environ.get('DATABASE_NAME')
BYBIT_API_KEY = environ.get('BYBIT_API_KEY')
BYBIT_SECRET = environ.get('BYBIT_SECRET')

SQL_QUERIES_FOLDER = 'sql'
