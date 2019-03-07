import sqlite3
import logging

logger = logging.getLogger(__name__)

DB_FILE = 'res/ip_cache.db'
TABLE = 'ip_country'

class IPCountryCache:
    '''Memory cache for IP address to country_name mapping.
    Supports persistence to db'''
    def __init__(self):
        self.cache = dict()
        self.conn = sqlite3.connect(DB_FILE)
        self.create_table()
        self.load()

    def add(self, ip_address, country):
        self.cache[ip_address] = country

    def get(self, ip_address):
        if ip_address in self.cache:
            return self.cache[ip_address]

        c = self.conn.cursor()
        sql = '''SELECT country_name FROM {0}
                 WHERE ip_address = ?'''.format(TABLE)
        c.execute(sql, (ip_address,))
        return c.fetchone()

    def persist(self):
        items = self.cache.items()
        if self.db_greater_than(items):
            return
        logger.info('Persist ip cache to db')
        c = self.conn.cursor()
        c.execute('DELETE FROM {0};'.format(TABLE))
        sql = '''INSERT INTO {0} 
                 VALUES (:ip_address, :country_name)'''.format(TABLE)
        c.executemany(sql, items)
        self.conn.commit()
        self.conn.close()

    def db_greater_than(self, items):
        c = self.conn.cursor()
        sql = '''SELECT count(*) FROM {0}'''.format(TABLE)
        c.execute(sql)
        return c.fetchone()[0] >= len(items)

    def load(self):
        c = self.conn.cursor()
        sql = '''SELECT * FROM {0}'''.format(TABLE)
        c.execute(sql)
        for row in c.fetchall():
            self.cache[row[0]] = row[1]

    def create_table(self):
        sql = '''CREATE TABLE IF NOT EXISTS {0}
                ( ip_address varchar(15) not null primary key,
                  country_name varchar(255) 
                )'''.format(TABLE)
        c = self.conn.cursor()
        c.execute(sql)