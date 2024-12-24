import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from curl_cffi import requests


def request_get(url, impersonate="chrome"):
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


def first_cache_then_download_for_catalog(url: str,
                                          output_dir,
                                          catalog_cache_prefix: str,
                                          catalog_html_cache_file,
                                          now_ts: int,
                                          impersonate="chrome"):
    logging.info(f"尝试下载公告目录: {url}")
    logging.info(f"创建数据保存目录: {output_dir}")
    os.makedirs(os.path.dirname(output_dir), exist_ok=True)
    catalog_content = None
    with os.scandir(output_dir) as it:
        for entry in it:
            if entry.is_file() and entry.name.startswith(catalog_cache_prefix):
                ts = entry.name.replace(catalog_cache_prefix, "").split(".")[0]
                if ts is not None:
                    # 缓存有效期1h
                    if now_ts - int(ts) < 60:
                        logging.info(f"一级目录还在缓存有效期内，读取本地缓存: {entry.path}")
                        with open(entry.path, "r") as f:
                            catalog_content = f.read()
                            break
                    else:
                        logging.info(f"一级目录缓存失效，清理缓存文件 {entry.path}")
                        os.remove(entry.path)
    if catalog_content is not None and len(catalog_content) > 0:
        return True, catalog_content
    # 新请求
    logging.info("一级目录还在缓存为空或失效，开始新请求")
    resp = request_get(url, impersonate)
    if resp.status_code < 200 or resp.status_code >= 300:
        return False, catalog_content
    cache_file = catalog_html_cache_file + str(now_ts) + ".html"
    with open(cache_file, "w") as f:
        catalog_content = resp.text
        f.write(catalog_content)
        logging.info(f"一级目录请求成功, 缓存结果 {cache_file}")
    return False, catalog_content


def first_cache_then_download(url: str,
                              output_dir,
                              article_cache_prefix: str,
                              article_title: str,
                              article_id: str,
                              release_time: int,
                              impersonate="chrome"):
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
