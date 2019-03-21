import unittest
import sqlite3
import os

import load
from load import Load

TEST_DB = 'test/test.db'

class TestLoad(unittest.TestCase):

    @classmethod
    def tearDown(cls):
        try:
            os.remove(TEST_DB)
        except FileNotFoundError:
            pass

    def test_connect_to_db(self):
        conn = load.connect_to_db(TEST_DB)
        # get cursor to test connection
        conn.cursor()

    def test_create_table(self):
        with sqlite3.connect(TEST_DB) as conn:
            load.create_table(conn, 'test_accounts')

            # check that table exists
            c = conn.cursor()
            sql = '''SELECT name FROM sqlite_master 
                    WHERE type='table' AND name=?'''
            c.execute(sql, ('test_accounts',))
            result = c.fetchone()
            assert result[0] == 'test_accounts'
    
    def test_create_table_twice(self):
        with sqlite3.connect(TEST_DB) as conn:
            load.create_table(conn, 'test_accounts')
            # call again, must not raise error 
            # as table is created using (IF NOT EXISTS) 
            load.create_table(conn, 'test_accounts')

    def test_load(self):
        data = [{'accountid':'1', 'gender':'female', 'age':42, 'country':'Germany', 
                 'extract_date':'2018-06-28', 'load_date':'2019-03-07', 'game':'foo'},
                {'accountid':'2', 'gender':'male', 'age':38, 'country':'United States', 
                 'extract_date':'2018-06-28', 'load_date':'2019-03-07', 'game':'bar'}]
        Load.load(data, 'test', TEST_DB)

        expected = [('foo', '1', 'female', 42, 'Germany', '2018-06-28', '2019-03-07'), 
                    ('bar', '2', 'male', 38, 'United States', '2018-06-28', '2019-03-07')]
        with sqlite3.connect(TEST_DB) as conn:
            c = conn.cursor()
            sql = '''SELECT * FROM test_accounts'''
            c.execute(sql)
            result = c.fetchall()
            self.assertEqual(result, expected)

    def test_load_replace(self):
        data1 = [{'accountid':'1', 'gender':'female', 'age':42, 'country':'Germany', 
                 'extract_date':'2018-06-28', 'load_date':'2019-03-07', 'game':'foo'},
                {'accountid':'2', 'gender':'MALE', 'age':38, 'country':'United States', 
                 'extract_date':'2018-06-28', 'load_date':'2019-03-07', 'game':'bar'}]
        data2 = [{'accountid':'1', 'gender':'female', 'age':42, 'country':'Germany', 
                 'extract_date':'2018-06-28', 'load_date':'2019-03-07', 'game':'foo'},
                {'accountid':'2', 'gender':'FEMALE', 'age':38, 'country':'United States', 
                 'extract_date':'2018-06-28', 'load_date':'2019-03-07', 'game':'bar'}]
        Load.load(data1, 'test', TEST_DB)

        # load again with record 2 data changed
        Load.load(data2, 'test', TEST_DB)

        expected = [('foo', '1', 'female', 42, 'Germany', '2018-06-28', '2019-03-07'), 
                    ('bar' ,'2', 'FEMALE', 38, 'United States', '2018-06-28', '2019-03-07')]
        with sqlite3.connect(TEST_DB) as conn:
            c = conn.cursor()
            sql = '''SELECT * FROM test_accounts'''
            c.execute(sql)
            result = c.fetchall()
            self.assertEqual(result, expected)
