import atexit
import logging
import sqlite3
import sys

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, g, jsonify, request
from binance.main import do_scrapy as scrapy_binance
from bitget.main import do_scrapy as scrapy_bitget
from bybit.main import do_scrapy as scrapy_bybit
from gate.main import do_scrapy as scrapy_gate

sys.path.append("..")
from exchange_announce import repository

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def scheduled_task(ex):
    if ex == 'BINANCE':
        scrapy_binance(0)
    elif ex == 'BITGET':
        scrapy_bitget(0)
    elif ex == 'BYBIT':
        scrapy_bybit(0)
    elif ex == 'GATE':
        scrapy_gate(0)
    else:
        logging.info("没有找到")


scheduler = BackgroundScheduler()
scheduler.add_job(func=scheduled_task, trigger="cron", second=1, args=['BINANCE'])
scheduler.add_job(func=scheduled_task, trigger="cron", second=2, args=['BITGET'])
scheduler.add_job(func=scheduled_task, trigger="cron", second=3, args=['BYBIT'])
scheduler.add_job(func=scheduled_task, trigger="cron", second=4, args=['GATE'])
if not scheduler.running:
    scheduler.start()
# 注册关闭事件，确保应用退出时调度器能够正常关闭
atexit.register(lambda: scheduler.shutdown())


def get_db():
    """获取数据库连接"""
    if 'db' not in g:
        g.db = repository.connect()
        g.db.row_factory = sqlite3.Row  # 使查询结果以字典形式返回
    return g.db


@app.teardown_appcontext
def close_db(error):
    """在请求结束时关闭数据库连接"""
    db = g.pop('db', None)
    if db is not None:
        db.close()


@app.route("/ping")
def hello_world():
    return "pong"


@app.route("/new-listing", methods=['GET'])
def new_listing():
    limit = request.args.get('limit', default=10, type=int)
    rows = repository.list_new_listing(get_db(), limit)
    return jsonify(rows)


# 启动应用
if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=7001, use_reloader = False, threaded = False)
