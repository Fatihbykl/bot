import psycopg2
import numpy


class Database:
    """PostgreSQL Database class"""

    def __init__(self, config):
        self.host = config.DATABASE_HOST
        self.username = config.DATABASE_USERNAME
        self.password = config.DATABASE_PASSWORD
        self.port = config.DATABASE_PORT
        self.dbname = config.DATABASE_NAME
        self.conn = None

    def connect(self):
        """Connect to a Postgres database"""

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

    def insert_row(self, _topic, _timestamp, _interval, _open, _close, _high, _low, _volume):
        self.connect()
        with self.conn.cursor() as cur:
            try:
                query = """INSERT INTO "KlineData" (topic, timestamp, interval, open, close, high, low, volume) VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s)"""
                values = (_topic, _timestamp, _interval, _open, _close, _high, _low, _volume)
                cur.execute(query, values)
                self.conn.commit()
                cur.close()
            except psycopg2.DatabaseError as e:
                print(e)
                raise e
            finally:
                print('Values inserted successfully.')

    def get_last_ohlc(self, _topic):
        self.connect()
        with self.conn.cursor() as cur:
            try:
                query = """SELECT "open","close","high","low" FROM "KlineData" WHERE topic=%s"""
                values = (_topic,)
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

