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
output_dir = os.path.join(current_dir, '..', 'output', 'gate', '')
CATALOG_CACHE_PREFIX = "catalog_cache_"
CATALOG_HTML_CACHE_FILE = os.path.join(output_dir, CATALOG_CACHE_PREFIX)


def parse_article(title, html, url, db_conn):
    logging.info(f"开始解析公告内容: {title}")
    title_pattern = r"已上線(\w+)永續合約交易"
    title_match = re.findall(title_pattern, title)
    if not title_match:
        return
    logging.info("疑似该公告为合约上线公告?, 进一步解析明细表")
    for identifier in title_match:
        base_coin = identifier
        logging.info(f"匹配到的左币: {identifier}")
        break
    selector = parsel.Selector(text=html)
    contents = selector.css(".article-dtl-content ::text").getall()
    swap_pattern = r"已上線(\w+)/USDT永續合約實盤交易"
    for content in contents:
        swap_match = re.findall(swap_pattern, content)
        if swap_match:
            # publish_time_pattern = r"article-details-title"
            new_listing_time = 0
            publish_time_str = selector.css(".article-details-base-info span ::text").get()
            logging.info(f"公告发布时间: {publish_time_str.strip()}")
            if publish_time_str is not None and len(publish_time_str) > 0:
                publish_time_str = publish_time_str.strip()
                dt = datetime.strptime(publish_time_str, "%Y-%m-%d %H:%M:%S UTC+8")
                tz = ZoneInfo("Asia/Shanghai")
                dt = dt.replace(tzinfo=tz)
                new_listing_time = int(dt.timestamp())
            logging.info(f"该公告确认为合约上线公告，执行入库: {base_coin}-USDT-SWAP, 上线时间: 已上线")
            repository.save_new_listing(db_conn, title, f"{base_coin}-USDT-SWAP", "GATE", new_listing_time, url)
            db_conn.commit()
            return


def follow_article(title, href, article_id, db_conn):
    logging.info(f"跟踪文章: {title} ---> {href} ---> {article_id}")
    article_url = "https://www.gate.io" + href
    cache, html = article_downloader.first_cache_then_download(article_url,
                                                               output_dir,
                                                               CATALOG_CACHE_PREFIX,
                                                               title,
                                                               article_id,
                                                               0,
                                                               impersonate="chrome_android")
    if not cache:
        logging.info("控制访问频率, sleep 3s")
        time.sleep(3)
    if html is not None and len(html) > 0:
        parse_article(title, html, article_url, db_conn)


def parse_catalog_html(html, db_conn):
    logging.info("开始解析一级目录")
    selector = parsel.Selector(text=html)
    articles = selector.css(".article-list-item-content a.article-list-item-title").getall()
    for article in articles:
        article_selector = parsel.Selector(text=article)
        title = article_selector.css("span::text").get()
        href = article_selector.css("::attr('href')").get()
        article_id = os.path.basename(href)
        follow_article(title, href, article_id, db_conn)


def do_scrapy(retry_cnt: int):
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - [%(pathname)s:%(lineno)d]: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    NOW = datetime.now()
    NOW_UNIX_TS = int(NOW.timestamp())
    logging.info(f"执行Gate上新公告抓取任务, 当前时间: {NOW}, ts: {NOW_UNIX_TS}, retry_cnt = {retry_cnt}")
    catalog_url = "https://www.gate.io/zh/announcements/newlisted"
    logging.info(f"查询公告一级目录索引, {catalog_url}")
    cache, catalog_html = article_downloader.first_cache_then_download_for_catalog(
        catalog_url,
        output_dir,
        CATALOG_CACHE_PREFIX,
        CATALOG_HTML_CACHE_FILE,
        NOW_UNIX_TS,
        impersonate="chrome_android"
    )
    db_conn = repository.connect()
    if catalog_html is not None and len(catalog_html) > 0:
        parse_catalog_html(catalog_html, db_conn)


if __name__ == '__main__':
    do_scrapy(0)
