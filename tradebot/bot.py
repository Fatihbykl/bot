import time
import logging
import numpy as np
import math
import threading
import redis
import json

import pybit.exceptions

import config
from datetime import datetime
from fractions import Fraction
from market import Coin
from db import Database
from market import ManageCoins
from talib import _ta_lib as talib
from pybit.unified_trading import HTTP, WebSocket
from confdata import Side, Position, StrategyInfo, TradeData
from decimal import Decimal, getcontext
from strategies.strategies import StmaADX, Supertrend
from pprint import pprint




class ManageBots:
    """ Class for managing bots. """

    instance = None

    def __new__(cls, db, coins):
        """ Code for Singleton. """

        if not isinstance(cls.instance, cls):
            cls.instance = super(ManageBots, cls).__new__(cls)
        return cls.instance

    def __init__(self, db, coins):
        self.db = db
        self.coins = coins
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        self.start_ws_executions_publish()

    def handle_executions(self, execution):
        symbol = execution['data'][0]['symbol']
        data = json.dumps(execution)
        self.redis.publish(channel=f'execution_{symbol}', message=data)

    def start_ws_executions_publish(self):
        ws = WebSocket(
            channel_type='private',
            api_key=config.BYBIT_API_KEY,
            api_secret=config.BYBIT_SECRET,
            testnet=True
        )
        ws.execution_stream(callback=self.handle_executions)

    def get_tradeable_coins(self):
        pass

    def start_bots(self):
        coin = self.coins.get_coin_object('ETHUSDT')
        coin.start_publish_data()
        strategy = Supertrend()
        bot = Bot(db=self.db, coin=coin, strategy=strategy, interval='1', r_value='0.1')
        bot.start()


class Bot:
    """ Class for trading. """

    def __init__(self,
                 db: Database,
                 coin: Coin,
                 interval,
                 r_value,
                 strategy,
                 ):
        self.coin = coin
        self.stopped = True
        self.db = db
        self.interval = interval

        self.strategy_info = StrategyInfo(
            r_value=r_value,
            strategy=strategy,
            sl_buy='0.98',
            sl_sell='1.02',
            tp_buy='1.01',
            tp_sell='0.99'
        )

        self.trade_data = TradeData(
            current_price='',
            tp_price='',
            sl_price='',
            candle_closed=False,
            ohlc_data=None,
            signal=Side.NO_SIGNAL
        )

        self.position = None
        self.leverage = 10

        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        self.pubsub_price = self.redis.pubsub()
        self.pubsub_execution = self.redis.pubsub()
        self.pubsub_price.subscribe(f'realtime_{coin.symbol}')
        self.pubsub_execution.subscribe(f'execution_{self.coin.symbol}')

        self.max_qty = None
        self.min_qty = None

        self.session = HTTP(
            testnet=True,
            api_key=config.BYBIT_API_KEY,
            api_secret=config.BYBIT_SECRET
        )

        self.set_instrument_info()
        self.set_leverage()

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s :: %(levelname)s :: %(message)s',
            filename='./logs/bybit.log'
        )
        getcontext().prec = 6

    def run(self):
        """ Main function that run forever. """
        
        self.ohlc_data = self.db.get_last_ohlc(_topic=self.coin.symbol, _interval=self.interval)
        while not self.stopped or self.position:  # and check if position open
            self.check_position()
            message = self.pubsub_price.get_message()
            if message and not message['data'] == 1:
                split = message['data'].split(b',')
                self.trade_data.current_price = split[0].decode("utf-8") 
                self.trade_data.candle_closed = split[1].decode("utf-8") 
                #print(f'Price: {self.trade_data.current_price}, is_closed: {self.trade_data.candle_closed}')
            
                if self.trade_data.candle_closed == 'True':
                    self.trade_data.ohlc_data = self.db.get_last_ohlc(_topic=self.coin.symbol, _interval=self.interval)
                    self.trade_data.signal = self.strategy_info.strategy.produce_signal(
                        self.trade_data.ohlc_data['close'],
                        self.trade_data.ohlc_data['high'],
                        self.trade_data.ohlc_data['low'],
                        1,
                        1
                    )
                    print(f'signal: {self.trade_data.signal}')
                    print(f'ohlc: {self.trade_data.ohlc_data}')

                if not self.position:
                    if self.trade_data.signal == Side.BUY:
                        self.place_market_order(Side.BUY, self.calculate_quantity())
                    elif self.trade_data.signal == Side.SELL:
                        self.place_market_order(Side.SELL, self.calculate_quantity())
                else:
                    if self.position.side == Side.BUY:
                        if self.trade_data.signal == Side.CLOSE:
                            self.close_position(size=1)
                        elif self.trade_data.signal == Side.SELL:
                            self.close_position()
                            self.place_market_order(Side.SELL, self.calculate_quantity())
                    elif self.position.side == Side.SELL:
                        if self.trade_data.signal == Side.CLOSE:
                            self.close_position(size=1)
                        elif self.trade_data.signal == Side.BUY:
                            self.close_position()
                            self.place_market_order(Side.BUY, self.calculate_quantity())

                
                time.sleep(0.3)

            

    def start(self):
        """ Start thread. """

        self.stopped = False
        t = threading.Thread(target=self.run)
        t.start()

    def set_instrument_info(self):
        """ Get symbol leverage and quantity info. """

        info = self.session.get_instruments_info(
            category="linear",
            symbol=self.coin.symbol,
        )
        self.max_qty = info['result']['list'][0]['lotSizeFilter']['maxOrderQty']
        self.min_qty = info['result']['list'][0]['lotSizeFilter']['minOrderQty']
        max_leverage = info['result']['list'][0]['leverageFilter']['maxLeverage']
        max_leverage = max_leverage.split('.')[0]

        if int(max_leverage) >= 50:
            self.leverage = 50
        else:
            self.leverage = int(max_leverage)

    def set_leverage(self):
        """ Set leverage to 50 or max leverage available. """

        try:
            self.session.set_leverage(
                category="linear",
                symbol=self.coin.symbol,
                buyLeverage=str(self.leverage),
                sellLeverage=str(self.leverage),
            )
        except pybit.exceptions.InvalidRequestError as e:
            print(e)
        finally:
            print('Leverage set to: ', self.leverage)

    def get_account_balance(self):
        """ Get USDT balance. """

        balance = self.session.get_wallet_balance(
            accountType="CONTRACT",
            coin='USDT'
        )

        balance = balance['result']['list'][0]['coin'][0]['equity']
        return balance

    def calculate_quantity(self):
        """ Calculate quantity from given R value. """

        balance = Decimal(self.get_account_balance())
        value = balance * Decimal(5)
        r = value * Decimal(self.strategy_info.r_value)
        raw_qty = r / Decimal(self.trade_data.current_price)
        qty = math.floor(raw_qty / Decimal(self.min_qty)) * Decimal(self.min_qty)
        return min(round(qty, 3), Decimal(self.max_qty))

    def place_market_order(self, side, quantity):
        """ Buy coin with market order. """

        if side == Side.BUY:
            stop_loss = Decimal(self.trade_data.current_price) * Decimal(self.strategy_info.sl_buy)
            take_profit = Decimal(self.trade_data.current_price) * Decimal(self.strategy_info.tp_buy)
        else:
            stop_loss = Decimal(self.trade_data.current_price) * Decimal(self.strategy_info.sl_sell)
            take_profit = Decimal(self.trade_data.current_price) * Decimal(self.strategy_info.tp_sell)

        order = self.session.place_order(
            category='linear',
            symbol=self.coin.symbol,
            side=side,
            orderType='Market',
            qty=quantity,
            stopLoss=stop_loss,
            tpslMode='Full'
        )
        tp_qty = round(Decimal(quantity) / 2, 3)
        tp_order = self.session.set_trading_stop(
            category='linear',
            symbol=self.coin.symbol,
            takeProfit=take_profit,
            tpTriggerBy='LastPrice',
            tpslMode='Partial',
            tpOrderType='Market',
            tpSize=tp_qty,
            positionIdx=0
        )
        
        return order

    def update_stoploss_to_entry(self):
        self.session.set_trading_stop(
            category="linear",
            symbol=self.coin.symbol,
            stopLoss=self.position.entry_price,
            slTriggerBy="LastPrice",
            tpslMode="Full",
            tpOrderType="Market",
            positionIdx=0
        )

    def close_position(self, size):
        """
            Close position or take profit from it.
            Size=1 means full position, Size=0.5 means close half of the position etc.
        """

        position_side = self.position.side
        if position_side == Side.BUY:
            position_side = Side.SELL
        else:
            position_side = Side.BUY
        qty = Decimal(self.position.size) * Decimal(size)
        order = self.session.place_order(
            category='linear',
            symbol=self.coin.symbol,
            side=position_side,
            qty=qty,
            orderType='Market',
            reduceOnly=True
        )
        self.update_position()
        return order

    def check_position(self):
        message = self.pubsub_execution.get_message()
        
        if message and not message['data'] == 1:
            msg = message['data'].decode("utf-8") 
            msg = json.loads(msg)
            closed_size = msg['data'][0]['closedSize']
            order_type = msg['data'][0]['stopOrderType']

            if closed_size == '0': # new position opened
                self.update_position()
            else: # tp or stoploss executed
                if order_type == 'StopLoss':
                    self.position = None
                elif order_type == 'TakeProfit':
                    self.update_position()
                    self.update_stoploss_to_entry()

            pprint(msg)

    def update_position(self):
        """ Update position parameters. """

        positions = self.session.get_positions(
            category="linear",
            symbol=self.coin.symbol,
        )
        position = positions['result']['list'][0]
        if Decimal(position['positionValue']) == 0:
            self.position = None
        elif not self.position:
            pos = Position(
                symbol=position['symbol'],
                side=position['side'],
                size=position['size'],
                entry_price=position['avgPrice'],
                created_time=datetime.now(),
                close_prices=[],
                value=position['positionValue']
            )
            self.position = pos
            if pos.side == Side.BUY:
                self.trade_data.sl_price = Decimal(pos.entry_price) * Decimal(self.strategy_info.sl_buy)
                self.trade_data.tp_price = Decimal(pos.entry_price) * Decimal(self.strategy_info.tp_buy)
            else:
                self.trade_data.sl_price = Decimal(pos.entry_price) * Decimal(self.strategy_info.sl_sell)
                self.trade_data.tp_price = Decimal(pos.entry_price) * Decimal(self.strategy_info.tp_sell)
        elif self.position.size != position['size']:
            self.position.size = position['size']
            self.position.value = position['positionValue']
            self.position.close_prices.append(self.trade_data.current_price)
            self.trade_data.sl_price = self.position.entry_price

