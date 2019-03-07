import unittest
import sqlite3
import os

from load import Load

TEST_DB = 'test.db'
class TestLoad(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.conn = sqlite3.connect(TEST_DB)

    @classmethod
    def tearDownClass(cls):
        os.remove(TEST_DB)

    def test_create_table(self):
        load = Load([])
        load.create_table()
