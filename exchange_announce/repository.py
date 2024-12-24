import sqlite3
from datetime import datetime
import jinja2


def connect():
    return sqlite3.connect('output/database.sqlite')


def save_new_listing(conn, title: str, symbol: str, exchange: str, time: int, url: str):
    cursor = conn.cursor()
    now = int(datetime.now().timestamp())
    cursor.execute('''
    insert into new_listing_symbol (title, symbol, `exchange`, new_listing_at, url, last_update_at, created_at) values (?, ?, ?, ?, ?, ?, ?)
    ON conflict(symbol, `exchange`) do update set title = ?, new_listing_at = ?, url = ?, last_update_at = ?
    where symbol = ? and `exchange` = ?
    ''', (title, symbol, exchange, time, url, now, now, title, time, url, now, symbol, exchange))


def list_new_listing(conn, limit, exchange, symbol):
    cursor = conn.cursor()
    where = ""
    binds = []
    if exchange is not None:
        where += " where exchange = ?"
        binds.append(exchange)
    if symbol is not None and len(where) > 0:
        where += ", symbol = ?"
        binds.append(symbol)
    if symbol is not None and len(where) <= 0:
        where += " where symbol = ?"
        binds.append(symbol)
    binds.append(limit)
    cursor.execute(f"select * from new_listing_symbol {where} order by created_at desc limit ?", binds)
    rows = cursor.fetchall()
    return [dict(row) for row in rows]