import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from curl_cffi import requests


def request_get(url, impersonate = "chrome"):
    r = requests.get(
        url,
        impersonate=impersonate,
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
        })
    return r


def first_cache_then_download(url: str,
                              output_dir,
                              article_cache_prefix: str,
                              article_title: str,
                              article_id: str,
                              release_time: int,
                              impersonate = "chrome"):
    logging.info(f"尝试下载公告: {url}")
    release_datatime = datetime.fromtimestamp(int(release_time) / 1000, tz=ZoneInfo("Asia/Shanghai"))
    date = release_datatime.strftime("%Y%m%d")
    logging.debug(f"当前日期目录为: {date}")
    cache_dir = os.path.join(output_dir, date, '')
    os.makedirs(os.path.dirname(cache_dir), exist_ok=True)
    artile_file = os.path.join(cache_dir, article_cache_prefix + article_id + ".html")
    try:
        with open(artile_file, "r") as f:
            article_content = f.read()
            logging.info(f"读取缓存公告, {artile_file}")
            cache = True
    except FileNotFoundError:
        logging.info(f"公告缓存不存在, 发起下载请求, {url}")
        body = request_get(url, impersonate)
        article_content = body.text
        with open(artile_file, "w") as f:
            f.write(article_content)
            logging.info(f"缓存公告: {article_title}, code={article_id}")
        cache = False

    return cache, article_content
