import ccxt

if __name__ == '__main__':
    exchange = ccxt.binanceusdm()
    exchange.fetch_funding_rate()
    markets = exchange.load_markets()
    print(markets['BTC/USDT:USDT'])