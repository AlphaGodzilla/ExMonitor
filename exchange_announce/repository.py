import sqlite3
from datetime import datetime


def connect():
    return sqlite3.connect('database.sqlite')


def save_new_listing(conn, title: str, symbol: str, exchange: str, time: int, url: str):
    cursor = conn.cursor()
    now = int(datetime.now().timestamp())
    cursor.execute('''
    insert into new_listing_symbol (title, symbol, `exchange`, new_listing_at, url, last_update_at, created_at) values (?, ?, ?, ?, ?, ?, ?)
    ON conflict(symbol, `exchange`) do update set title = ?, new_listing_at = ?, url = ?, last_update_at = ?
    where symbol = ? and `exchange` = ?
    ''', (title, symbol, exchange, time, url, now, now, title, time, url, now, symbol, exchange))


def list_new_listing(conn, limit):
    cursor = conn.cursor()
    cursor.execute('''
    select * from new_listing_symbol order by created_at desc limit ?
    ''', (limit,))
    rows = cursor.fetchall()
    return [dict(row) for row in rows]
