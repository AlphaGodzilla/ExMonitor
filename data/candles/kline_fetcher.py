import json
from pathlib import Path

import ccxt
from ccxt.base.exchange import Exchange
from datetime import datetime, date, time, timezone, timedelta
import csv


def fetch_ohlcv(exchange, symbol_info, timeframe, from_date: date, until_date: date):
    since = parse_date_ts_mills(from_date)
    until = parse_date_ts_mills(until_date)
    inst_id = symbol_info['id']
    all_ohlcv_list = []
    while since < until:
        ohlcv_list = exchange.fetch_ohlcv(inst_id, timeframe, limit=100, since=since)
        if len(ohlcv_list) > 0:
            all_ohlcv_list += ohlcv_list
            since = ohlcv_list[len(ohlcv_list) - 1][0] + 1
        else:
            break
    return all_ohlcv_list


def build_filename(exchange: Exchange, symbol_info, timeframe, date1: date):
    id1 = exchange.name
    inst_id = symbol_info['id']
    time1 = date1.strftime("%Y%m%d")
    return "output/data/" + id1 + "_" + inst_id + "_" + timeframe + "_" + time1


def save_ohlc(exchange, symbol_info, date1: date, all_ohlcv_list):
    pass


def sync_ohlcv_by_date(exchange, symbol: str, timeframe: str, from_date: date):
    exchange.load_markets()
    inst = exchange.markets[symbol]
    # 判断文件是否已经存在
    filename = build_filename(exchange, inst, timeframe, from_date)
    file_path = Path(filename + ".csv")
    file_path_abs = str(file_path.resolve())
    if file_path.exists() and file_path.is_file():
        # 数据已经存在，无需同步
        print("数据已存在，无需同步: " + file_path_abs)
        return
    # 执行同步逻辑
    ohlcv_list = fetch_ohlcv(exchange, inst, timeframe, from_date, from_date + timedelta(days=1))
    # 封装数据
    with open(file_path_abs, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['开', '高', '低', '收', '量'])
        writer.writerows(ohlcv_list)
    print("下载完成: " + file_path_abs)


def parse_date_ts_mills(value: str | date | datetime):
    """
    :param value:  2024-06-10
    :return:
    """
    date_part = None
    if isinstance(value, datetime):
        date_part = value.date()
    elif isinstance(value, str):
        date_part = datetime.strptime(value, '%Y-%m-%d').date()
    elif isinstance(value, date):
        date_part = value
    else:
        raise ValueError("The value type must one of [date,datetime,str]")
    tz = timezone(timedelta(hours=8))
    return int(datetime.combine(date_part, time(0, 0, 0), tz).timestamp() * 1000)


def parse_date(value):
    return date.fromtimestamp(parse_date_ts_mills(value) / 1000)


if __name__ == '__main__':
    # okx = ccxt.okx()
    # okx.enableRateLimit = True
    # okx.verbose = True
    bybit = ccxt.bybit()
    bybit.verbose = True
    from_date = parse_date('2024-10-28')
    today = parse_date(datetime.today())
    fetch_date = from_date
    # print("fetch_date, " + str(type(fetch_date)))
    # print("today, " + str(type(today)))
    while fetch_date <= today:
        sync_ohlcv_by_date(bybit, 'GOAT/USDT:USDT', '1m', fetch_date)
        fetch_date += timedelta(days=1)
