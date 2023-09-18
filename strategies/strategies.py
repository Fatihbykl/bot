from talib import _ta_lib as talib
from tradebot.confdata import Side
import numpy as np


class StmaADX:

    def produce_signal(self, open, high, low, close, periods, multiplier, length, T3a1):
        trend = self.supertrend_ma(open, high, low, close, periods, multiplier, length, T3a1)
        adx = talib.ADX(high, low, close, 14)
        buy_signal = (trend == 1) & (np.roll(trend, 1) == -1)
        sell_signal = (trend == -1) & (np.roll(trend, 1) == 1)

        if adx > 20:
            if buy_signal:
                return Side.BUY
            elif sell_signal:
                return Side.SELL
            else:
                return None

    def supertrend_ma(self, open, high, low, close, periods, multiplier, length, T3a1):
        changeATR = True  # Change this to False if needed

        # Assuming you have calculated MA, atr, up, dn, trend, up1, dn1, buySignal, sellSignal, ohlc4
        atr = talib.ATR(high, low, close)
        ohlc4 = (open + high + low + close) / 4
        MA = talib.EMA(close, length)

        # T3e1 = talib.EMA(close, length)
        # T3e2 = talib.EMA(T3e1, length)
        # T3e3 = talib.EMA(T3e2, length)
        # T3e4 = talib.EMA(T3e3, length)
        # T3e5 = talib.EMA(T3e4, length)
        # T3e6 = talib.EMA(T3e5, length)
        # T3c1 = -T3a1 * T3a1 * T3a1
        # T3c2 = 3 * T3a1 * T3a1 + 3 * T3a1 * T3a1 * T3a1
        # T3c3 = -6 * T3a1 * T3a1 - 3 * T3a1 - 3 * T3a1 * T3a1 * T3a1
        # T3c4 = 1 + 3 * T3a1 + T3a1 * T3a1 * T3a1 + 3 * T3a1 * T3a1
        # MA = T3c1 * T3e6 + T3c2 * T3e5 + T3c3 * T3e4 + T3c4 * T3e3

        # Calculate atr2
        atr2 = np.convolve(np.abs(ohlc4 - np.roll(ohlc4, 1)), np.ones(periods) / periods, mode='full')
        atr2 = atr2[periods - 1:]

        # Calculate atr
        atr = atr if not changeATR else atr2

        # Calculate up and dn
        up = MA - multiplier * atr
        up1 = np.where(np.isnan(np.roll(up, 1)), up, np.roll(up, 1))
        up = np.where(np.roll(ohlc4, 1) > up1, np.maximum(up, up1), up)

        dn = MA + multiplier * atr
        dn1 = np.where(np.isnan(np.roll(dn, 1)), dn, np.roll(dn, 1))
        dn = np.where(np.roll(ohlc4, 1) < dn1, np.minimum(dn, dn1), dn)

        # Calculate trend
        trend = np.ones(len(ohlc4))
        trend = np.where(np.isnan(np.roll(trend, 1)), trend, np.roll(trend, 1))
        trend = np.where((trend == -1) & (ohlc4 > dn1), 1, np.where((trend == 1) & (ohlc4 < up1), -1, trend))

        return trend
