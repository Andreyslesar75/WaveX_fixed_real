
from binance.client import Client
from config import Config

client = Client(Config.BINANCE_API_KEY, Config.BINANCE_API_SECRET)

# ---------------------------------------------------------------------------------------------
# Установка кредитного плеча
def SetLeverage (list_valut, n):
    for i in range (len(list_valut)):
        client.futures_change_leverage(symbol=list_valut[i], leverage=n)
    print ()
    print ('Кредитные плечи всех валют установленны в', n)


# Get list_valut - Список валют по индексам
def GetListValut(tickers, test_on, test_valut, limit_valut):
    #global buy_flags_array, flag_array, volum_vhod, volum_vhod_2, candles_end_flags, block_valutes, short_array, mid_bik_candle, mid_med_candle, max_price_arr, v15_speed_array

    list_valut = []
    for i in range(len(tickers)):
        s = tickers[i]
        valute = s['symbol']
        #volum = float(s['quoteVolume'])
        if valute.find('_') == -1 and valute.find('USDT') > 0:
            list_valut.append(valute)
        if len(list_valut) == limit_valut and test_on == 1:
            break

    if test_on == 1:
        try:
            if test_valut > -1:
                print ('Введен индекс тестовой валюты =', test_valut)
        except TypeError:
            try:
                test_valut = list_valut.index(test_valut)
            except ValueError:
                print('Тестовой валюты в массиве НЕТ!... Совсем!!!')
                print ('.....Сброс номера тестовой валюты в 0')
                test_valut = 0
                #stop_V = 1
        print ('Control valuta -', list_valut[test_valut])
        #print (list_valut)

    return list_valut

tickers = client.futures_symbol_ticker()
list_valut = GetListValut(tickers, 0, 0, 0)

SetLeverage (list_valut, 1)