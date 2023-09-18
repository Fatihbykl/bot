import datetime
import logging
import psycopg2
from psycopg2 import pool
import numpy
import pandas as pd


class Database:
    """ PostgreSQL Database class. """

    instance = None

    def __new__(cls, config):
        """ Code for Singleton. """

        if not isinstance(cls.instance, cls):
            cls.instance = super(Database, cls).__new__(cls)
        return cls.instance

    def __init__(self, config):
        self.host = config.DATABASE_HOST
        self.username = config.DATABASE_USERNAME
        self.password = config.DATABASE_PASSWORD
        self.port = config.DATABASE_PORT
        self.dbname = config.DATABASE_NAME
        self.logger = logging.getLogger(__name__)
        self.conn = None

        self.connect()

    def connect(self):
        """ Connect to a Postgres database. """

        if self.conn is None:
            try:
                self.conn = pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=100,
                    host=self.host,
                    user=self.username,
                    password=self.password,
                    port=self.port,
                    dbname=self.dbname
                )
            except psycopg2.DatabaseError as e:
                self.logger.exception(e)
                raise e

    def insert_row_kline(self, _topic, _timestamp, _interval, _open, _close, _high, _low, _volume):
        """ Insert row to KlineData table. """

        connection = self.conn.getconn()
        with connection.cursor() as cur:
            try:
                query = """INSERT INTO "KlineData" (topic, timestamp, open, close, high, low, volume, interval) VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s)"""
                values = (_topic, _timestamp, _open, _close, _high, _low, _volume, _interval)
                cur.execute(query, values)
                connection.commit()
                cur.close()
                self.conn.putconn(conn=connection)
            except psycopg2.DatabaseError as e:
                self.logger.exception(e)
                raise e

    def insert_multiple_row_kline(self, values):
        """ Insert multiple data to KlineData table. """

        connection = self.conn.getconn()
        with connection.cursor() as cur:
            try:
                args = ','.join(cur.mogrify("(%s,%s,%s, %s, %s, %s, %s, %s)", i).decode('utf-8')
                                for i in values)
                query = "INSERT INTO \"KlineData\" VALUES " + (args)
                cur.execute(query, values)
                connection.commit()
                cur.close()
                self.conn.putconn(conn=connection)
            except psycopg2.DatabaseError as e:
                self.logger.exception(e)
                raise e

    def upsert_row_coindata(self, topic, rsi, natr, volume, interval, adx, timestamp=datetime.datetime.now()):
        """ Update data from CoinData table. """

        connection = self.conn.getconn()
        with connection.cursor() as cur:
            try:
                query = """INSERT INTO "CoinData" (topic, timestamp, interval, rsi, natr, adx, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (topic) DO UPDATE SET rsi=excluded.rsi, natr=excluded.natr, volume=excluded.volume,
                timestamp=excluded.timestamp, interval=excluded.interval, adx=excluded.adx"""
                values = (topic, timestamp, interval, rsi, natr, adx, volume)
                cur.execute(query, values)
                connection.commit()
                cur.close()
                self.conn.putconn(conn=connection)
            except psycopg2.DatabaseError as e:
                self.logger.exception(e)
                raise e

    def insert_from_csv(self, tablename, csv_path):
        """ Insert data from csv file. """

        connection = self.conn.getconn()
        with connection.cursor() as cur:
            try:
                with open(csv_path) as file:
                    cur.copy_from(file=file, table=tablename, sep=',')
                    connection.commit()
                cur.close()
                self.conn.putconn(conn=connection)
            except psycopg2.DatabaseError as e:
                self.logger.exception(e)
                raise e

    def truncate_coininfo_table(self):
        """ Remove rows from CoinInfo table. """

        connection = self.conn.getconn()
        with connection.cursor() as cur:
            try:
                query = """ TRUNCATE TABLE "CoinInfo" """
                cur.execute(query)
                connection.commit()
                cur.close()
                self.conn.putconn(conn=connection)
            except psycopg2.DatabaseError as e:
                self.logger.exception(e)
                raise e

    def get_coindata_values(self, interval):
        """ Get last ohlc data from given topic and interval. """

        connection = self.conn.getconn()
        with connection.cursor() as cur:
            try:
                query = """SELECT * FROM "CoinData" WHERE interval=\'{0}\'""".format(interval)
                df = pd.read_sql_query(query, connection)
                cur.close()
                self.conn.putconn(conn=connection)
                return df
            except psycopg2.DatabaseError as e:
                self.logger.exception(e)
                raise e

    def get_last_ohlc(self, _topic, _interval):
        """ Get last ohlc data from given topic and interval. """

        connection = self.conn.getconn()
        with connection.cursor() as cur:
            try:
                query = """SELECT "open","close","high","low" FROM "KlineData" WHERE topic=%s AND interval=%s
                        ORDER BY "timestamp" DESC """
                values = (_topic, str(_interval))
                cur.execute(query, values)
                open = []
                close = []
                high = []
                low = []
                for i in cur.fetchall():
                    open.append(float(i[0]))
                    close.append(float(i[1]))
                    high.append(float(i[2]))
                    low.append(float(i[3]))
                cur.close()
                self.conn.putconn(conn=connection)
                return {'open': numpy.asarray(open), 'close': numpy.asarray(close),
                        'high': numpy.asarray(high), 'low': numpy.asarray(low)}
            except psycopg2.DatabaseError as e:
                self.logger.exception(e)
                raise e
