from talib import _ta_lib as talib
from tradebot.confdata import Side
import numpy as np
import math


class StmaADX:

    def produce_signal(self, open, high, low, close, periods, multiplier, length, T3a1):
        trend = self.supertrend_ma(open, high, low, close, periods, multiplier, length, T3a1)
        #adx = talib.ADX(high, low, close, 14)
        buy_signal = (trend == 1) & (np.roll(trend, 1) == -1)
        sell_signal = (trend == -1) & (np.roll(trend, 1) == 1)

        #if adx[-1] > 20:
        if buy_signal[-1]:
            return Side.BUY
        elif sell_signal[-1]:
            return Side.SELL
        else:
            return Side.NO_SIGNAL

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


class Supertrend:

    def produce_signal(self, close_array, high_array, low_array, atr_period, atr_multiplier):
        supertrend = self.generateSupertrend(close_array, high_array, low_array, atr_period, atr_multiplier)

        son_kapanis = close_array[-1]
        onceki_kapanis = close_array[-2]

        son_supertrend_deger = supertrend[-1]
        onceki_supertrend_deger = supertrend[-2]

        # renk yeşile dönüyor, trend yükselişe geçti
        if son_kapanis > son_supertrend_deger and onceki_kapanis < onceki_supertrend_deger:
            return Side.BUY

        # renk kırmızıya dönüyor, trend düşüşe geçti
        elif son_kapanis < son_supertrend_deger and onceki_kapanis > onceki_supertrend_deger:
            return Side.SELL
        
        else:
            return Side.NO_SIGNAL

    def generateSupertrend(self, close_array, high_array, low_array, atr_period, atr_multiplier):

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