import logging
import os
import re
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import parsel

sys.path.append("..")
from exchange_announce import article_downloader, repository

current_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(current_dir, 'output', '')
ARTICLE_CACHE_PREFIX = "article_cache_"


def download_article_detail(article):
    title = article["annTitle"]
    id = article["annId"]
    url = article["annUrl"]
    cTime = article["cTime"]
    return article_downloader.first_cache_then_download(url, output_dir, ARTICLE_CACHE_PREFIX, title, id, int(cTime),
                                                        "chrome_android")


def parse_article(url, name, html, db_conn):
    logging.info(f"开始解析公告内容: {name}")
    selector = parsel.Selector(text=html)
    r = selector.css("div[class^=\"ArticleDetails_actice_details_main\"] ::text").getall()
    all_text = ""
    base_coin = ""
    for i in r:
        i = i.strip()
        if i == "\n" or i == "\r" or i == "\r\n" or i == "\t" or i == '':
            continue
        if len(i) >= 0:
            # print(i)
            all_text += i + "\n"
    pattern0 = r"(\w+)\s+U本位永续合约："
    matches0 = re.findall(pattern0, all_text)
    if matches0:
        for identifier in matches0:
            base_coin = identifier
            logging.info(f"匹配到的左币: {identifier}")
            break
        if all_text.find(base_coin.strip() + "USDT") != -1 and all_text.find("上线") != -1 and all_text.find(
                "合约交易") != -1:
            logging.info("疑似该公告为合约上线公告?, 进一步解析明细表")
            tds = selector.css("table tr td ::text").getall()
            if tds is not None and len(tds) > 0:
                new_listing_time = None
                base_coin = None
                px_coin = None
                for idx in range(0, len(tds), 2):
                    # print(f"{tds[idx]} ---> {tds[idx + 1]}")
                    key = tds[idx]
                    val = tds[idx + 1]
                    if key == "上线时间":
                        val = val.strip()
                        dt = datetime.strptime(val, "%Y年%m月%d日 %H:%M（UTC+8）")
                        dt = dt.astimezone(ZoneInfo("Asia/Shanghai"))
                        new_listing_time = int(dt.timestamp())
                    if key == "合约标的":
                        base_coin = val.strip().upper()
                    if key == "结算资产":
                        px_coin = val.strip().upper()
                if db_conn is not None and base_coin is not None and px_coin is not None and new_listing_time is not None:
                    logging.info(
                        f"该公告确认为合约上线公告，执行入库: {base_coin}-{px_coin}-SWAP, 上线时间: {new_listing_time}")
                    repository.save_new_listing(db_conn, name, f"{base_coin}-{px_coin}-SWAP", "BITGET",
                                                new_listing_time, url)
                    db_conn.commit()


def do_scrapy(retry_cnt: int):
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    NOW = datetime.now()
    NOW_UNIX_TS = int(NOW.timestamp())
    logging.info(f"执行BitGet上新公告抓取任务, 当前时间: {NOW}, ts: {NOW_UNIX_TS}, retry_cnt = {retry_cnt}")
    catalog_url = "https://api.bitget.com/api/v2/public/annoucements?annType=coin_listings&language=zh_CN"
    logging.info(f"查询公告列表接口, {catalog_url}")
    catalog_body = article_downloader.request_get(catalog_url)
    catalog_data = catalog_body.json()
    # print(catalog_data)
    items = catalog_data["data"]
    db_conn = repository.connect()
    if items is not None and len(items) > 0:
        for i in items:
            cache, html = download_article_detail(i)
            if not cache:
                logging.info("控制访问频率, sleep 3s")
                time.sleep(3)
            parse_article(i["annUrl"], i["annTitle"], html, db_conn)
    logging.info("关闭数据库连接")
    db_conn.close()
    logging.info("结束")


if __name__ == '__main__':
    do_scrapy(0)
