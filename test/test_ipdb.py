import unittest
import sqlite3
import os

import ipdb
from ipdb import IPDb

TEST_DB = 'test/test_ip.db'

class TestLoad(unittest.TestCase):

    @classmethod
    def tearDown(cls):
        try:
            os.remove(TEST_DB)
        except FileNotFoundError:
            pass

    def test_connect_to_db(self):
        ipdb = IPDb()
        conn = ipdb.connect_to_db(TEST_DB)
        # get cursor to test connection
        conn.cursor()

    def test_create_table(self):
        db = IPDb(TEST_DB)
        db.create_table()
    
        with sqlite3.connect(TEST_DB) as conn:
            # check that table exists
            c = conn.cursor()
            sql = '''SELECT name FROM sqlite_master 
                    WHERE type='table' AND name=?'''
            c.execute(sql, (ipdb.TABLE_NAME,))
            result = c.fetchone()
            assert result[0] == ipdb.TABLE_NAME
    
    def test_add(self):
        db = IPDb(TEST_DB)
        db.add('1.1.1.1', 'Fooland')
        with sqlite3.connect(TEST_DB) as conn:
            c = conn.cursor()
            sql = '''SELECT country_name
                     FROM {0} WHERE ip_address=?'''.format(ipdb.TABLE_NAME)
            c.execute(sql, ('1.1.1.1',))
            result = c.fetchone()[0]
            self.assertEqual(result, 'Fooland')

    def test_get(cls):
        db = IPDb(TEST_DB)
        db.create_table()
        db.add('1.1.1.1','Barland')
        assert db.get('1.1.1.1') == 'Barland'