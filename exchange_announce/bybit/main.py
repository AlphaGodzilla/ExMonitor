import hashlib
import json
import logging
import os
import re
import sys
import time
from datetime import datetime

import parsel

sys.path.append("..")
from exchange_announce import article_downloader, repository

current_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(current_dir, 'output', '')
ARTICLE_CACHE_PREFIX = "article_cache_"


def download_article_detail(article):
    title = article['title']
    url = article['url']
    publish_at = article['publishTime']
    sha1 = hashlib.sha1()
    sha1.update(url.encode('utf-8'))
    article_id = sha1.hexdigest()
    return article_downloader.first_cache_then_download(url, output_dir, ARTICLE_CACHE_PREFIX, title, article_id,
                                                        int(publish_at),
                                                        "chrome_android")


def parse_article(url, title, html, db_conn):
    logging.info(f"开始解析公告内容: {title}")
    selector = parsel.Selector(text=html)
    script_js = selector.css("#__NEXT_DATA__ ::text").get()
    # print(script)
    script = json.loads(script_js)
    article = script['props']
    if article is not None:
        article = article['pageProps']
        if article is not None:
            article = article['articleDetail']
            if article is not None:
                desc = article['description']
                date = article['date']
                pattern = r"已上線\s?(\w+)USDT\s?永續合約"
                match = re.findall(pattern, desc)
                if match:
                    base_coin = None
                    logging.info("疑似该公告为合约上线公告?, 进一步解析明细表")
                    for identifier in match:
                        base_coin = identifier
                        logging.info(f"匹配到的左币: {identifier}")
                        break
                    if base_coin is not None and date is not None:
                        dt = datetime.fromisoformat(date)
                        new_listing_time = int(dt.timestamp())
                        logging.info(
                            f"该公告确认为合约上线公告，执行入库: {base_coin}-USDT-SWAP, 上线时间: {new_listing_time}")
                        repository.save_new_listing(db_conn, title, f"{base_coin}-USDT-SWAP", "BYBIT", new_listing_time,
                                                    url)
                        db_conn.commit()


def do_scrapy(retry_cnt: int):
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    NOW = datetime.now()
    NOW_UNIX_TS = int(NOW.timestamp())
    logging.info(f"执行ByBit上新公告抓取任务, 当前时间: {NOW}, ts: {NOW_UNIX_TS}, retry_cnt = {retry_cnt}")
    catalog_url = "https://api.bybit.com/v5/announcements/index?locale=zh-TW&type=new_crypto"
    logging.info(f"查询公告列表接口, {catalog_url}")
    catalog_body = article_downloader.request_get(catalog_url)
    catalog_data = catalog_body.json()
    # print(catalog_data)
    result = catalog_data["result"]
    db_conn = repository.connect()
    if result is not None and type({}) == type(result):
        result = result["list"]
        if result is not None and type([]) == type(result):
            for ann in result:
                title = ann['title']
                url = ann['url']
                publish_at = ann['publishTime']
                cache, html = download_article_detail(ann)
                if not cache:
                    logging.info("控制访问频率, sleep 3s")
                    time.sleep(3)
                parse_article(url, title, html, db_conn)
            logging.info("关闭数据库连接")
            db_conn.close()
            logging.info("结束")
            return
    logging.info("一级目录页数据无法解析为公告列表，请人工介入")


if __name__ == '__main__':
    do_scrapy(0)
