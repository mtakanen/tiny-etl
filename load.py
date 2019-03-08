import sqlite3

import logging

DB_FILENAME = 'db/game_etl.db'
TABLE_NAME = 'accounts'

logger = logging.getLogger(__name__)

class Load():
    '''Loads accounts to database. Stores hb and wwc account data into same table.
    If colliding accountid is deteceted old record is replaced (i.e. no support for 
    historical versioning of data).
    '''
    @staticmethod
    def load(data, game, db=DB_FILENAME):
        logger.info('Load data to {0}'.format(db))
        
        with connect_to_db(db) as conn: 
            create_table(conn, TABLE_NAME)
            c = conn.cursor()
            sql = '''INSERT OR REPLACE INTO {0} 
                    VALUES (:game, :accountid, :gender, :age, 
                    :country, :extract_date, :load_date)'''.format(TABLE_NAME)
            c.executemany(sql, data)
            conn.commit()


def create_table(conn, table_name):
    sql = '''CREATE TABLE IF NOT EXISTS {0}
            (  game VARCHAR(3) NOT NULL,
               accountid VARCHAR(255) NOT NULL PRIMARY KEY,
               gender VARCHAR(6),
               age INT,
               country VARCHAR(255),
               extract_date DATE NOT NULL,
               load_date DATE NOT NULL
            )'''.format(table_name)
    conn.execute(sql)

def connect_to_db(db_filename):
    try:
        conn = sqlite3.connect(db_filename)
        return conn
    except sqlite3.Error as err:
        logger.error('Connection to db {0} failed: {1}'.format(DB_FILENAME, err))
        exit(1)