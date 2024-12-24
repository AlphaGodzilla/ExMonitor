import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import parsel
from curl_cffi import requests

sys.path.append("..")
from exchange_announce import repository
from exchange_announce.binance.proc import get_token


def do_request(url, token):
    r = requests.get(
        url,
        impersonate="chrome",
        headers={
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "priority": "u=0, i",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?1",
            "sec-ch-ua-platform": "\"Android\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "cookie": "aws-waf-token=" + token,
        })
    return r


current_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(current_dir, '..', 'output', 'binance', '')
os.makedirs(os.path.dirname(output_dir), exist_ok=True)

WAF_TOKE_FILE = os.path.join(output_dir, 'waf_token.txt')
CATALOG_CACHE_PREFIX = "catalog_cache_"
CATALOG_HTML_CACHE_FILE = os.path.join(output_dir, CATALOG_CACHE_PREFIX)
ARTICLE_CACHE_PREFIX = "article_cache_"


def parse_article(url, name, content, db_conn):
    logging.info(f"开始解析公告内容: {name}")
    selector = parsel.Selector(text=content)
    items = selector.css(".richtext-paragraph ::text").getall()
    match = False
    for i in items:
        pattern = r'关于(\w+)\s+U本位永续合约的更多信息如下表所示'
        matches = re.findall(pattern, i)
        if matches:
            match = True
            for identifier in matches:
                logging.info(f"匹配到的标识符列表: {identifier}")
            break

    if match:
        logging.info(f"发现U本位合约上新公告: {name}")
        first_table = selector.css("table:nth-of-type(1)").get()
        selector = parsel.Selector(text=first_table)
        items = selector.css("tr td .richtext-text ::text").getall()
        inst_id = None
        new_listing_time = None
        base_coin = None
        px_coin = None
        for i in range(0, len(items), 2):
            key = items[i]
            val = items[i + 1]
            if key == "U本位永续合约":
                inst_id = val.strip().upper()
            if key == "上线时间":
                val = val.strip()
                dt = datetime.strptime(val, "%Y年%m月%d日%H:%M（东八区时间）")
                tz = ZoneInfo("Asia/Shanghai")
                dt = dt.replace(tzinfo=tz)
                dt = dt.astimezone(tz)
                new_listing_time = int(dt.timestamp())
            if key == "结算资产":
                px_coin = val.strip().upper()
                if inst_id is not None:
                    base_coin = inst_id.replace(px_coin, "")
        repository.save_new_listing(db_conn, name, f"{base_coin}-{px_coin}-SWAP", "BINANCE", new_listing_time, url)
        db_conn.commit()
    else:
        logging.info(f"没有匹配上新合约交易对: {name}")


def follow_article_details(article, token, conn):
    if article is None:
        return
    code = article['code']
    article_url = "https://www.binance.com/zh-CN/support/announcement/" + code
    releaseDate = article["releaseDate"]
    release_datatime = datetime.fromtimestamp(int(releaseDate) / 1000, tz=ZoneInfo("Asia/Shanghai"))
    date = release_datatime.strftime("%Y%m%d")
    logging.debug(f"当前日期目录为: {date}")
    cache_dir = os.path.join(output_dir, date, '')
    os.makedirs(os.path.dirname(cache_dir), exist_ok=True)
    # 查询本地缓存
    artile_file = os.path.join(cache_dir, ARTICLE_CACHE_PREFIX + code + ".html")
    article_content = None
    try:
        with open(artile_file, "r") as f:
            article_content = f.read()
            logging.info(f"读取缓存公告, {artile_file}")
    except FileNotFoundError:
        logging.info(f"公告缓存不存在, 发起读取请求, {article_url}")
        r = do_request(article_url, token)
        article_content = r.text
        with open(artile_file, "w") as f:
            f.write(article_content)
            logging.info(f"缓存公告: {article['title']}, code={code}")
        logging.info("控制访问频率, sleep 3s")
        time.sleep(3)
    parse_article(article_url, article['title'], article_content, conn)


def do_scrapy(retry_cnt: int):
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    NOW = datetime.now()
    NOW_UNIX_TS = int(NOW.timestamp())
    logging.info(f"执行Binance上新公告抓取任务, 当前时间: {NOW}, ts: {NOW_UNIX_TS}, retry_cnt = {retry_cnt}")
    token = ""
    try:
        with open(WAF_TOKE_FILE, "r") as waf_token:
            token = waf_token.read()
            logging.info(f"从本地缓存中读取token: {token}")
    except FileNotFoundError:
        logging.info("waf_token文件缓存不存在,执行获取流程")
        token = get_token()
        logging.info(f"waf_token已获取: {token}")
        with open(WAF_TOKE_FILE, "w") as waf_token:
            waf_token.write(token)
            logging.info("刷新文件缓冲区，确保文件写入磁盘")
            waf_token.flush()
            logging.info("waf_token保存完成")
    catalog_url = "https://www.binance.com/zh-CN/support/announcement/new-cryptocurrency-listing?c=48&navId=48"
    logging.info(f"抓取上线列表一级目录: {catalog_url}, [优先读取缓存]")

    catalog_content = None
    with os.scandir(output_dir) as it:
        for entry in it:
            if entry.is_file() and entry.name.startswith(CATALOG_CACHE_PREFIX):
                ts = entry.name.replace(CATALOG_CACHE_PREFIX, "").split(".")[0]
                if ts is not None:
                    # 缓存有效期1h
                    if NOW_UNIX_TS - int(ts) < 60 * 60:
                        logging.info(f"一级目录还在缓存有效期内，读取本地缓存: {entry.path}")
                        with open(entry.path, "r") as f:
                            catalog_content = f.read()
                            break
                    else:
                        logging.info(f"一级目录缓存失效，清理缓存文件 {entry.path}")
                        os.remove(entry.path)

    if catalog_content is None or len(catalog_content) <= 0:
        logging.info("一级目录还在缓存为空或失效，开始新请求")
        r = do_request(catalog_url, token)
        cache_file = CATALOG_HTML_CACHE_FILE + str(NOW_UNIX_TS) + ".html"
        with open(cache_file, "w") as f:
            catalog_content = r.text
            if len(catalog_content) <= 0 or r.status_code == 202:
                logging.info("waf-token过期, status_code = 202 or catalog_content is empty")
                os.remove(WAF_TOKE_FILE)
                logging.info(f"删除token文件: {WAF_TOKE_FILE}，准备重试")
                if retry_cnt >= 3:
                    logging.error("重试多次后还是失败，放弃执行请人工介入")
                    return
                # 递归调用自身函数，执行重试
                do_scrapy(retry_cnt + 1)
                return
            f.write(catalog_content)
            logging.info(f"一级目录请求成功, 缓存结果 {cache_file}")

    logging.info("开始解析一级目录")
    selector = parsel.Selector(text=catalog_content)
    script_app_data = selector.css("#__APP_DATA::text").get()
    if len(script_app_data) <= 0:
        logging.info("目标数据不存在，无法继续执行，请人工介入")
        return
    try:
        json_app_data = json.loads(script_app_data)
    except json.JSONDecodeError:
        logging.info("目标数据无法解析文json，无法继续执行，请人工介入")
        return
    articles = json_app_data['appState']["loader"]["dataByRouteId"]["d34e"]["catalogDetail"]["articles"]

    conn = repository.connect()

    for article in articles:
        follow_article_details(article, token, conn)
    logging.info("关闭数据库连接")
    conn.close()
    logging.info("结束")


if __name__ == '__main__':
    do_scrapy(0)
