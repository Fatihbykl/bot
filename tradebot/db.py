import datetime

import psycopg2
import numpy


class Database:
    """ PostgreSQL Database class. """

    def __init__(self, config):
        self.host = config.DATABASE_HOST
        self.username = config.DATABASE_USERNAME
        self.password = config.DATABASE_PASSWORD
        self.port = config.DATABASE_PORT
        self.dbname = config.DATABASE_NAME
        self.conn = None

    def connect(self):
        """ Connect to a Postgres database. """

        if self.conn is None:
            try:
                self.conn = psycopg2.connect(
                    host=self.host,
                    user=self.username,
                    password=self.password,
                    port=self.port,
                    dbname=self.dbname
                )
            except psycopg2.DatabaseError as e:
                print(e)
                raise e
            finally:
                print('Connection opened successfully.')

    def insert_row_kline(self, _topic, _timestamp, _interval, _open, _close, _high, _low, _volume):
        self.connect()
        with self.conn.cursor() as cur:
            try:
                query = """INSERT INTO "KlineData" (topic, timestamp, open, close, high, low, volume, interval) VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s)"""
                values = (_topic, _timestamp, _open, _close, _high, _low, _volume, _interval)
                cur.execute(query, values)
                self.conn.commit()
                cur.close()
            except psycopg2.DatabaseError as e:
                print(e)
                raise e
            finally:
                print('Values inserted successfully.')

    def insert_multiple_row_kline(self, values):
        """ Insert multiple data to KlineData table. """

        self.connect()
        with self.conn.cursor() as cur:
            try:
                args = ','.join(cur.mogrify("(%s,%s,%s, %s, %s, %s, %s, %s)", i).decode('utf-8')
                                for i in values)
                query = "INSERT INTO \"KlineData\" VALUES " + (args)
                cur.execute(query, values)
                self.conn.commit()
                cur.close()
            except psycopg2.DatabaseError as e:
                print(e)
                raise e
            finally:
                print('Values inserted successfully.')

    def insert_row_coindata(self, topic, interval, rsi, natr, volume, timestamp=datetime.datetime.now()):
        """ Insert data to CoinData table. """

        self.connect()
        with self.conn.cursor() as cur:
            try:
                query = """INSERT INTO "CoinData" (topic, rsi, natr, volume, timestamp, interval) 
                VALUES(%s, %s, %s, %s, %s, %s)"""
                values = (topic, rsi, natr, volume, timestamp, interval)
                cur.execute(query, values)
                self.conn.commit()
                cur.close()
            except psycopg2.DatabaseError as e:
                print(e)
                raise e
            finally:
                print('Values inserted successfully.')

    def update_row_coindata(self, topic, rsi, natr, volume, timestamp=datetime.datetime.now()):
        """ Update data from CoinData table. """

        self.connect()
        with self.conn.cursor() as cur:
            try:
                query = """UPDATE "CoinData" SET rsi=%s, natr=%s, volume=%s, timestamp=%s WHERE topic=%s"""
                values = (rsi, natr, volume, timestamp, topic)
                cur.execute(query, values)
                self.conn.commit()
                cur.close()
            except psycopg2.DatabaseError as e:
                print(e)
                raise e
            finally:
                print('Values inserted successfully.')

    def get_last_ohlc(self, _topic, _interval):
        """ Get last ohlc data from given topic and interval. """

        self.connect()
        with self.conn.cursor() as cur:
            try:
                query = """SELECT "open","close","high","low" FROM "KlineData" WHERE topic=%s AND interval=%s
                        ORDER BY "timestamp" """
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
                return {'open': numpy.asarray(open), 'close': numpy.asarray(close),
                        'high': numpy.asarray(high), 'low': numpy.asarray(low)}
            except psycopg2.DatabaseError as e:
                print(e)
                raise e
            finally:
                print('Data fetched successfully.')
