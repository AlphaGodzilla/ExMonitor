import json
import re
import time
from pathlib import Path

import ccxt
import requests
from ccxt.base.exchange import Exchange

exchanges = [
    ccxt.okx(),
    ccxt.binance(),
    ccxt.bitget(),
    ccxt.mexc(),
    ccxt.bybit(),
    ccxt.gate()
]
coins = set({})
all_coins_file = "output/coins.json"
coins_metadata_dir = "output/metadata/"
Path(coins_metadata_dir).mkdir(parents=True, exist_ok=True)


def load_all_coins():
    # 加载交易对
    for exchange in exchanges:
        # exchange.verbose = True
        exchange.rateLimit = True
        exchange.load_markets()
        for market in exchange.markets.values():
            if market['type'] != 'spot' and market['type'] != 'swap':
                continue
            coins.add(market['base'])
            coins.add(market['quote'])
        print("完成同步: " + exchange.id)
    # 保存进文件
    with open(all_coins_file, 'w') as file:
        file.write(json.dumps(list(coins)))
        print("所有coin写入完成, " + all_coins_file)


def read_all_coins_file():
    with open(all_coins_file, 'r', encoding='utf-8') as file:
        content = file.read()
    all_coins = json.loads(content)
    print("加载all_coins, size=" + str(len(all_coins)))
    return all_coins


def fetch_meta_data(batch_coins: [str]):
    headers = {
        'X-CMC_PRO_API_KEY': '9f4a4b18-f6ed-46d9-828d-4a7326cd2058'
    }
    params = {
        'symbol': ','.join(batch_coins),
        'skip_invalid': 'true'
    }
    resp = requests.get('https://pro-api.coinmarketcap.com/v2/cryptocurrency/info', headers=headers, params=params)
    print("查询结果: " + resp.text)
    body = resp.json()
    data = body['data']
    return data


def parse_resp_meta_data(data):
    result = []
    for coin_key in data:
        coins1 = data[coin_key]
        if len(coins1) <= 0:
            continue
        coins1 = list(filter(lambda x: x['category'] == 'coin' or x['category'] == 'token', coins1))
        if len(coins1) <= 0:
            continue
        result.append(coins1[0])
    return result


def batch_download_meta_info(all_coins: [str], batch_size: int):
    batch_coins = []
    total_count = len(all_coins)
    crt_count = 0
    symbol_pattern = r'^[a-zA-Z0-9]+$'
    for coin in all_coins:
        match = bool(re.match(symbol_pattern, coin))
        if not match:
            print("非法的symbol,放弃查询:", coin)
            continue
        if len(batch_coins) >= batch_size:
            # 开始请求
            print("批数量到达, 开始请求, batch=" + str(len(batch_coins)))
            resp_data = fetch_meta_data(batch_coins)
            match_coins = parse_resp_meta_data(resp_data)
            print("正在解析数据1, match_coins=" + str(len(match_coins)))
            for match_coin in match_coins:
                # 写入文件
                save_meta(match_coin['symbol'], match_coin)
            batch_coins.clear()
            print("当前同步进度: " + str((crt_count / total_count) * 100) + "%")
            print("控频暂停3s")
            time.sleep(3)
            print("结束暂停,继续执行")
        else:
            crt_count += 1
            file = Path(coins_metadata_dir + coin + ".json")
            if file.exists():
                print("元数据已存在: " + coin)
            else:
                batch_coins.append(coin)
    # 处理最后的数据
    if len(batch_coins) > 0:
        resp_data = fetch_meta_data(batch_coins)
        match_coins = parse_resp_meta_data(resp_data)
        print("正在解析数据1, match_coins=" + str(len(match_coins)))
        for match_coin in match_coins:
            # 写入文件
            save_meta(match_coin['symbol'], match_coin)
        batch_coins.clear()


def save_meta(symbol, all_meta_data):
    filename = coins_metadata_dir + symbol + ".json"
    with open(filename, mode='w', encoding='utf-8') as file:
        file.write(json.dumps(all_meta_data))
    print(filename + ", 文件保存完成")


# 执行脚本
# 生成all coin步骤
all_coins_path = Path(all_coins_file)
if all_coins_path.exists() and all_coins_path.is_file():
    print(all_coins_file + "已存在, 跳过同步all coin步骤")
else:
    load_all_coins()

# 同步原数据
all_coins_metadata_path = Path(coins_metadata_dir)
if all_coins_metadata_path.exists() and all_coins_metadata_path.is_file():
    print(coins_metadata_dir + "已存在，跳过fetch meta步骤")
else:
    all_coins = read_all_coins_file()
    batch_download_meta_info(all_coins, 100)
