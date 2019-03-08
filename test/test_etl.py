import unittest
import logging
from datetime import datetime
import sqlite3
import os

import etl
import transform

logging.disable(logging.CRITICAL)

TEST_DB = 'test/test.db'
TEST_DIR = 'test'


class TestEtl(unittest.TestCase):

    @classmethod
    def tearDown(cls):
        try:
            os.remove(TEST_DB)
        except FileNotFoundError:
            pass
    
    def test_parse_args(self):
        extract_date_str = '2019-03-01'
        args = etl.parse_args(['hb', extract_date_str])
        extract_date = datetime.strptime(extract_date_str, transform.EXTRACT_DATE_FORMAT).date()
        assert args[0] == 'hb' and args[1] == extract_date

    def test_parse_args_raises(self):
        extract_date_str = '2019-03-01'
        with self.assertRaises(SystemExit):
            etl.parse_args(['foo', extract_date_str])

    def test_parse_args_raises2(self):
        extract_date_str = '20190301'
        with self.assertRaises(SystemExit):
            etl.parse_args(['hb', extract_date_str])

    @classmethod
    def test_etl_hb(cls):
        extract_date = datetime.strptime('2019-03-02', transform.EXTRACT_DATE_FORMAT).date()
        data_dir = os.path.join(TEST_DIR, etl.DATA_DIR)
        etl.etl('hb', extract_date, data_dir=data_dir, db=TEST_DB)

        conn = sqlite3.connect(TEST_DB)
        c = conn.cursor()   
        c.execute('SELECT count(*) FROM accounts')
        assert c.fetchone()[0] == 10

    @classmethod
    def test_etl_wwc(cls):
        extract_date = datetime.strptime('2019-03-02', transform.EXTRACT_DATE_FORMAT).date()
        data_dir = os.path.join(TEST_DIR, etl.DATA_DIR)
        etl.etl('wwc', extract_date, data_dir=data_dir, db=TEST_DB)

        conn = sqlite3.connect(TEST_DB)
        c = conn.cursor()   
        c.execute('SELECT count(*) FROM accounts')
        assert c.fetchone()[0] == 5

