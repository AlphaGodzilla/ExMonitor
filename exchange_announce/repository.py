import sqlite3


def connect():
    return sqlite3.connect('database.sqlite')

def save_new_listing(conn, symbol: str, exchange: str, time: int, url: str):
    cursor = conn.cursor()
    cursor.execute('''
    insert into new_listing_symbol (symbol, `exchange`, new_listing_at, url) values (?, ?, ?, ?)
    ON conflict(symbol, `exchange`) do update set new_listing_at = ?, url = ?
    where symbol = ? and `exchange` = ?
    ''', (symbol, exchange, time, url, time, url, symbol, exchange))