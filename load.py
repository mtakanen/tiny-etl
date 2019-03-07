import sqlite3
import logging

logger = logging.getLogger(__name__)

DB_FILENAME = 'db/game_etl.db'
TABLE_SUFFIX = '_accounts'

conn = sqlite3.connect(DB_FILENAME)


class Load():

    @staticmethod
    def load(data, game):
        logger.info('Load data to db')
        table_name = '{0}{1}'.format(game, TABLE_SUFFIX)
        Load.create_table(table_name)
        c = conn.cursor()
        sql = '''INSERT INTO {0} 
                 VALUES (:id, :gender, :age, 
                 :country, :data_date, :load_date)'''.format(table_name)
        c.executemany(sql, data)
        conn.commit()
        conn.close()

    @staticmethod
    def delete_data(table_name, data_date):
        logger.info('Delete existing data on {0}'.format(data_date))
        c = conn.cursor()
        sql = '''DELETE FROM {0}
                 WHERE data_date = ?
                 AND game = ?'''.format(table_name)
        c.execute(sql, (data_date))       

    @staticmethod
    def create_table(table_name):
        sql = '''CREATE TABLE IF NOT EXISTS {0}
                (  id varchar(255) not null primary key,
                    gender varchar(6),
                    age int,
                    country varchar(255),
                    data_date date not null,
                    load_date date not null
                )'''.format(table_name)
        conn.execute(sql)