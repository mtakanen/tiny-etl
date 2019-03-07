import sqlite3

import logging

DB_FILENAME = 'db/game_etl.db'
TABLE_SUFFIX = '_accounts'

logger = logging.getLogger(__name__)

class Load():

    @staticmethod
    def load(data, game):
        logger.info('Load data to {0}'.format(DB_FILENAME))
        table_name = '{0}{1}'.format(game, TABLE_SUFFIX)

        with connect_to_db(DB_FILENAME) as conn: 
            create_table(conn, table_name)
            c = conn.cursor()
            sql = '''INSERT OR REPLACE INTO {0} 
                    VALUES (:accountid, :gender, :age, 
                    :country, :extract_date, :load_date)'''.format(table_name)
            c.executemany(sql, data)
            conn.commit()


def create_table(conn, table_name):
    sql = '''CREATE TABLE IF NOT EXISTS {0}
            (  accountid varchar(255) not null primary key,
               gender varchar(6),
               age int,
               country varchar(255),
               extract_date date not null,
               load_date date not null
            )'''.format(table_name)
    conn.execute(sql)

def connect_to_db(db_filename):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_filename)
        return conn
    except sqlite3.Error as err:
        logger.error('Connection to db {0} failed: {1}'.format(DB_FILENAME, err))