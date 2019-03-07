import sqlite3
import logging

logger = logging.getLogger(__name__)

DB_FILENAME = 'db/game_etl.db'
TABLE_NAME = 'users'

conn = sqlite3.connect(DB_FILENAME)


class Load():

    @staticmethod
    def load(data):
        logger.info('Load data to db')
        #delete_data(self.data_date, self.game)
        c = conn.cursor()
        sql = '''INSERT INTO {0} 
                 VALUES (:game, :id, :gender, :age, 
                 :country, :data_date, :load_date)'''.format(TABLE_NAME)
        c.executemany(sql, data)
        conn.commit()
        conn.close()

    @staticmethod
    def delete_data(data_date, game):
        logger.info('Delete existing data on {0}'.format(data_date))
        c = conn.cursor()
        sql = '''DELETE FROM {0}
                 WHERE data_date = ?
                 AND game = ?'''.format(TABLE_NAME)
        c.execute(sql, (data_date, game))       

    @staticmethod
    def create_schema():
        sql = '''CREATE TABLE IF NOT EXISTS {0}
                    ( game varchar(3) not null,
                    id varchar(255) not null primary key,
                    gender varchar(6),
                    age int,
                    country varchar(255),
                    data_date date not null,
                    load_date date not null
                    )'''.format(TABLE_NAME)
        conn.execute(sql)