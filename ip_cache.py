import sqlite3
import logging

logger = logging.getLogger(__name__)

DB_FILENAME = 'db/ip_cache.db'
TABLE_NAME = 'ip_country'

class IPCache:
    '''Database cache for IP address to country_name mapping.'''

    def __init__(self, db=DB_FILENAME):
        self.conn = self.connect_to_db(db)
        self.create_table()

    def add(self, ip_address, country):
        c = self.conn.cursor()
        sql = '''INSERT OR REPLACE INTO {0} 
                 VALUES (?, ?)'''.format(TABLE_NAME)
        c.execute(sql, (ip_address,country))
        self.conn.commit()

    def get(self, ip_address):
        country = None
        c = self.conn.cursor()
        sql = '''SELECT country_name FROM {0}
                 WHERE ip_address = ?'''.format(TABLE_NAME)
        c.execute(sql, (ip_address,))
        result = c.fetchone()
        if result:
            country = result[0]        
        return country

    def create_table(self):
        sql = '''CREATE TABLE IF NOT EXISTS {0}
                ( ip_address varchar(15) not null primary key,
                  country_name varchar(255) 
                )'''.format(TABLE_NAME)
        c = self.conn.cursor()
        c.execute(sql)

    def connect_to_db(self, db_filename):
        """ create a database connection to the SQLite database
            specified by the db_file
        :param db_file: database file
        :return: Connection object or None
        """
        try:
            conn = sqlite3.connect(db_filename)
            return conn
        except sqlite3.Error as err:
            logger.error('Connect to db {0} failed: {1}'.format(db_filename, err))
        return None