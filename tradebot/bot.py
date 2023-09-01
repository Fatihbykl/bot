import time
import logging
import numpy as np
import math
import threading

import pybit.exceptions

import config
from datetime import datetime
from fractions import Fraction
from market import Coin
from db import Database
from market import ManageCoins
from talib import _ta_lib as talib
from pybit.unified_trading import HTTP
from confdata import Side, Position
from decimal import Decimal, getcontext


class ManageBots:
    """ Class for managing bots. """

    instance = None

    def __new__(cls, db):
        """ Code for Singleton. """

        if not isinstance(cls.instance, cls):
            cls.instance = super(ManageBots, cls).__new__(cls)
        return cls.instance

    def __init__(self, db):
        self.db = db

    def get_tradeable_coins(self):
        pass

    def start_bots(self):
        coins = ManageCoins()
        btc = coins.get_coin_object('ASTRUSDT')
        bot = Bot(self.db, btc, '1', '0.1')
        bot.start()


class Bot:
    """ Class for trading. """

    def __init__(self,
                 db: Database,
                 coin: Coin,
                 interval,
                 r_value,
                 trade_value=2000
                 ):
        self.coin = coin
        self.trade_value = trade_value
        self.r_value = r_value
        self.stopped = True
        self.db = db
        self.interval = interval

        self.buy_position = None
        self.sell_position = None

        self.leverage = 10
        self.stop_loss_buy = '0.98'
        self.stop_loss_sell = '1.02'

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
            filename='bybit.log'
        )
        getcontext().prec = 6

    def run(self):
        """ Main function that run forever. """

        qty = self.calculate_quantity()
        self.place_market_order(Side.BUY, qty)
        print(self.buy_position)
        time.sleep(10)
        self.close_position(self.buy_position, 0.5)
        time.sleep(10)
        self.close_position(self.buy_position, 1)
        print(self.buy_position)

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
        self.max_qty = float(info['result']['list'][0]['lotSizeFilter']['maxOrderQty'])
        self.min_qty = float(info['result']['list'][0]['lotSizeFilter']['minOrderQty'])
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
        r = value * Decimal(self.r_value)
        raw_qty = r / Decimal(self.coin.current_price[0])
        qty = math.floor(raw_qty / Decimal(self.min_qty)) * Decimal(self.min_qty)
        return min(qty, self.max_qty)

    def place_market_order(self, side, quantity):
        """ Buy coin with market order. """

        stop_loss = None
        if side == Side.BUY:
            stop_loss = Decimal(self.coin.current_price[0]) * Decimal(self.stop_loss_buy)
        else:
            stop_loss = Decimal(self.coin.current_price[0]) * Decimal(self.stop_loss_sell)

        order = self.session.place_order(
            category='linear',
            symbol=self.coin.symbol,
            side=side,
            orderType='Market',
            qty=quantity,
            stopLoss=stop_loss
        )
        self.update_position()
        return order

    def close_position(self, position, size):  # size=1: Close full position, size=0.5 half of the postition etc.
        """
            Close position or take profit from it.
            Size=1 means full position, Size=0.5 means close half of the position etc.
        """

        position_side = position.side
        if position_side == Side.BUY:
            position_side = Side.SELL
        else:
            position_side = Side.BUY
        qty = Decimal(position.size) * Decimal(size)
        order = self.session.place_order(
            category='linear',
            symbol=self.coin.symbol,
            side=position_side,
            qty=qty,
            orderType='Market',
            reduceOnly=True
        )
        if size == 1:
            if position.side == Side.BUY:
                self.buy_position = None
            else:
                self.sell_position = None
        else:
            self.update_position()
        return order

    def update_position(self):
        """ Update position parameters. """

        positions = self.session.get_positions(
            category="linear",
            symbol=self.coin.symbol,
        )
        for position in positions['result']['list']:
            if position['side'] == Side.BUY:
                if not self.buy_position:
                    pos = Position(
                        symbol=position['symbol'],
                        side=position['side'],
                        size=position['size'],
                        entry_price=position['avgPrice'],
                        created_time=datetime.now(),
                        close_prices=[],
                        value=position['positionValue']
                    )
                    self.buy_position = pos
                elif self.buy_position.size != position['size']:
                    self.buy_position.size = position['size']
                    self.buy_position.value = position['positionValue']
                    self.buy_position.close_prices.append(self.coin.current_price[0])
            else:
                if not self.sell_position:
                    pos = Position(
                        symbol=position['symbol'],
                        side=position['side'],
                        size=position['size'],
                        entry_price=position['avgPrice'],
                        created_time=datetime.now(),
                        close_prices=[],
                        value=position['positionValue']
                    )
                    self.sell_position = pos
                elif self.sell_position.size != position['size']:
                    self.sell_position.size = position['size']
                    self.sell_position.value = position['positionValue']
                    self.sell_position.close_prices.append(self.coin.current_price[0])

    def produce_signal(self, close_array, high_array, low_array, atr_period, atr_multiplier):
        """ Produce signal from calculated supertrend. """

        supertrend = self.generate_supertrend(close_array, high_array, low_array, atr_period, atr_multiplier)
        close = close_array[-1]
        p_close = close_array[-2]
        st = supertrend[-1]
        p_st = supertrend[-2]

        if close > st and p_close < p_st:
            return 'B'
        if close < st and p_close > p_st:
            return 'S'

    @staticmethod
    def generate_supertrend(close_array, high_array, low_array, atr_period, atr_multiplier):
        """ Calculate supertrend. """

        atr = talib.ATR(high_array, low_array, close_array, atr_period)
        previous_final_upperband = 0
        previous_final_lowerband = 0
        final_upperband = 0
        final_lowerband = 0
        previous_close = 0
        previous_supertrend = 0
        supertrend = []
        supertrendc = 0

        for i in range(0, len(close_array)):
            if np.isnan(close_array[i]):
                pass
            else:
                highc = high_array[i]
                lowc = low_array[i]
                atrc = atr[i]
                closec = close_array[i]

                if math.isnan(atrc):
                    atrc = 0

                basic_upperband = (highc + lowc) / 2 + atr_multiplier * atrc
                basic_lowerband = (highc + lowc) / 2 - atr_multiplier * atrc

                if basic_upperband < previous_final_upperband or previous_close > previous_final_upperband:
                    final_upperband = basic_upperband
                else:
                    final_upperband = previous_final_upperband

                if basic_lowerband > previous_final_lowerband or previous_close < previous_final_lowerband:
                    final_lowerband = basic_lowerband
                else:
                    final_lowerband = previous_final_lowerband

                if previous_supertrend == previous_final_upperband and closec <= final_upperband:
                    supertrendc = final_upperband
                else:
                    if previous_supertrend == previous_final_upperband and closec >= final_upperband:
                        supertrendc = final_lowerband
                    else:
                        if previous_supertrend == previous_final_lowerband and closec >= final_lowerband:
                            supertrendc = final_lowerband
                        elif previous_supertrend == previous_final_lowerband and closec <= final_lowerband:
                            supertrendc = final_upperband

                supertrend.append(supertrendc)

                previous_close = closec
                previous_final_upperband = final_upperband
                previous_final_lowerband = final_lowerband
                previous_supertrend = supertrendc
        return supertrend
