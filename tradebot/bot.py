import time
import logging
import numpy as np
import math
import threading
import redis
import json
import logging

import pybit.exceptions

import config
from datetime import datetime
from fractions import Fraction
from market import Coin
from db import Database
from market import ManageCoins
from talib import _ta_lib as talib
from pybit.unified_trading import HTTP, WebSocket
from confdata import Side, Position, StrategyInfo, TradeData, PaperTrade
from decimal import Decimal, getcontext
from strategies.strategies import StmaADX, Supertrend
from pprint import pprint
from dataclasses import asdict




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
        #self.start_ws_executions_publish()

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
        bot = Bot(db=self.db, coin=coin, strategy=strategy, interval='1', r_value='0.1', paper_trade=True)
        bot.start()


class Bot:
    """ Class for trading. """

    def __init__(self,
                 db: Database,
                 coin: Coin,
                 interval,
                 r_value,
                 strategy,
                 paper_trade=False
                 ):
        self.coin = coin
        self.stopped = True
        self.db = db
        self.interval = interval
        self.logger = logging.getLogger() 


        # Paper trade
        if paper_trade:
            self.paper_trade = paper_trade
        else:
            self.paper_trade = None
        
        self.trades = []
        self.paper_initial_balance = '1000'
        self.paper_value = '1000'
        self.paper_pos_count = 0
        self.paper_win = 0
        self.paper_lose = 0
        self.win_rate = 0

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
            candle_closed=False,
            ohlc_data=None,
            signal=Side.NO_SIGNAL
        )

        self.position = None
        self.leverage = 10

        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        self.pubsub_price = self.redis.pubsub()
        self.pubsub_execution = self.redis.pubsub()
        self.pubsub_price.subscribe(f'realtime_{coin.symbol}_{self.interval}')
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

        getcontext().prec = 6

    def run(self):
        """ Main function that run forever. """
        
        self.ohlc_data = self.db.get_last_ohlc(_topic=self.coin.symbol, _interval=self.interval)
        while not self.stopped or self.position:  # and check if position open
            if not self.paper_trade:
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


                
                if self.paper_trade:
                    if not self.position:
                        if self.trade_data.signal == Side.BUY:
                            self.paper_open_position(Side.BUY)
                        elif self.trade_data.signal == Side.SELL:
                            self.paper_open_position(Side.SELL)
                    else:
                        if self.position.side == Side.BUY:
                            if self.trade_data.signal == Side.CLOSE:
                                self.paper_close_position()
                            elif self.trade_data.signal == Side.SELL:
                                self.paper_close_position()
                                self.paper_open_position(Side.SELL)
                        elif self.position.side == Side.SELL:
                            if self.trade_data.signal == Side.CLOSE:
                                self.paper_close_position()
                            elif self.trade_data.signal == Side.BUY:
                                self.paper_close_position()
                                self.paper_open_position(Side.BUY)

                if self.paper_pos_count >= 1:
                    self.win_rate = Decimal(100) * (Decimal(self.paper_win) / (Decimal(self.paper_win) + Decimal(self.paper_lose)))
                    self.print_paper_trades()
                    break
                """
                elif not self.position:
                    if self.trade_data.signal == Side.BUY:
                        self.place_market_order(Side.BUY, self.calculate_quantity())
                    elif self.trade_data.signal == Side.SELL:
                        self.place_market_order(Side.SELL, self.calculate_quantity())
                else:
                    if self.position.side == Side.BUY:
                        if self.trade_data.signal == Side.CLOSE:
                            self.close_position(size=1)
                        elif self.trade_data.signal == Side.SELL:
                            qty = Decimal(self.position.size) * 2
                            self.place_market_order(Side.SELL, qty)
                    elif self.position.side == Side.SELL:
                        if self.trade_data.signal == Side.CLOSE:
                            self.close_position(size=1)
                        elif self.trade_data.signal == Side.BUY:
                            qty = Decimal(self.position.size) * 2
                            self.place_market_order(Side.BUY, qty)
                """
                
                #time.sleep(0.3)

            
    def print_paper_trades(self):
        trade_list = json.dumps(self.trades)
        backtest_infos = json.dumps({ 'Positions': str(self.paper_pos_count), 'Wins': str(self.paper_win), 'Losses': str(self.paper_lose), 'Win Rate': str(self.win_rate) })
        with open('./logs/backtest_results.txt', 'w') as jf:
            jf.write(backtest_infos)
            jf.write('\n')
            jf.write(trade_list)

    def paper_open_position(self, side):
        if side == Side.BUY:
            sl_price=Decimal(self.trade_data.current_price) * Decimal(self.strategy_info.sl_buy),
            tp_price=Decimal(self.trade_data.current_price) * Decimal(self.strategy_info.tp_buy)
        else:
            sl_price=Decimal(self.trade_data.current_price) * Decimal(self.strategy_info.sl_sell),
            tp_price=Decimal(self.trade_data.current_price) * Decimal(self.strategy_info.tp_sell)

        self.position = Position(
            symbol=self.coin.symbol,
            side=side,
            size='0',
            created_time=str(datetime.now()),
            entry_price=str(self.trade_data.current_price),
            value=str(self.paper_value)
        )
        self.paper_trade = PaperTrade(
            symbol=self.coin.symbol,
            side=Side.SELL,
            realized_pl='0',
            entry_price=self.position.entry_price,
            closed_prices=[],
            sl_price=str(sl_price),
            tp_price=str(tp_price),
            created_time=str(datetime.now())
        )

    def paper_close_position(self):
        if self.position.side == Side.BUY:
            chg = Decimal(self.trade_data.current_price) / Decimal(self.position.entry_price)
        else:
            chg = Decimal(self.position.entry_price) / Decimal(self.trade_data.current_price)
        self.paper_trade.closed_prices.append(self.trade_data.current_price)
        self.paper_trade.realized_pl = str(
                Decimal(self.paper_trade.realized_pl) + (Decimal(self.paper_value) * Decimal(chg) - Decimal(self.paper_value))
            )
        self.paper_initial_balance = Decimal(self.paper_initial_balance) + Decimal(self.paper_trade.realized_pl)
        self.position = None
        self.trades.append(asdict(self.paper_trade))
        if Decimal(self.paper_trade.realized_pl) > 0:
            self.paper_win += 1
        else:
            self.paper_lose += 1
        self.paper_pos_count += 1

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
            qty=str(quantity),
            stopLoss=str(stop_loss),
            tpslMode='Full'
        )
        tp_qty = round(Decimal(quantity) / 2, 3)
        tp_order = self.session.set_trading_stop(
            category='linear',
            symbol=self.coin.symbol,
            takeProfit=str(take_profit),
            tpTriggerBy='LastPrice',
            tpslMode='Partial',
            tpOrderType='Market',
            tpSize=str(tp_qty),
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
            qty=str(qty),
            orderType='Market',
            reduceOnly=True
        )
        return order

    def check_position(self):
        message = self.pubsub_execution.get_message()
        
        if message and not message['data'] == 1:
            msg = message['data'].decode("utf-8") 
            msg = json.loads(msg)
            order_type = msg['data'][0]['stopOrderType']
            pprint(msg)
            self.update_position()
            self.logger.info(self.position.entry_price)
            if order_type == 'PartialTakeProfit':
                self.update_stoploss_to_entry()
                self.logger.info(f'TakeProfit: {self.position.side}, {self.position.entry_price}')

    def update_position(self):
        """ Update position parameters. """

        positions = self.session.get_positions(
            category="linear",
            symbol=self.coin.symbol,
        )
        position = positions['result']['list'][0]
        self.logger.info(f'pos val: {Decimal(position["positionValue"])}')
        if Decimal(position['positionValue']) == 0:
            self.position = None
        else:
            pos = Position(
                symbol=position['symbol'],
                side=position['side'],
                size=position['size'],
                entry_price=position['avgPrice'],
                created_time=datetime.now(),
                value=position['positionValue']
            )
            self.position = pos

