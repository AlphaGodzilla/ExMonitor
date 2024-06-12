import ccxt

if __name__ == '__main__':
    exchange = ccxt.binanceusdm()
    markets = exchange.load_markets()
    print(markets['BTC/USDT:USDT'])