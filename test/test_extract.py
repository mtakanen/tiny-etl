import unittest
import logging
from datetime import datetime

import extract
import transform
from extract import Extract

logging.disable(logging.CRITICAL)

class TestExtractUtils(unittest.TestCase):

    def test_read_csv(self):
        # test file has 10 lines of csv
        generator = extract.read_csv('test/data/hb/2019/03/02/test.csv')
        ids = [item['id'] for item in generator]
        expected = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
        self.assertEqual(ids, expected)

    def test_read_csv_limit(self):
        generator = extract.read_csv('test/data/hb/2019/03/02/test.csv', limit=1)
        assert len(list(generator)) == 1

    def test_read_json(self):
        # test file has 5 lines of json
        generator = extract.read_json('test/data/wwc/2019/03/02/test.json')
        salts = [item['login']['salt'] for item in generator]
        expected = ["U9LROYgO", "WluZWbQl", "oM76mHyR", "5SuhtPDF",  "fi5vYtOi"]
        self.assertEqual(salts, expected)

    def test_read_json_limit(self):
        generator = extract.read_json('test/data/wwc/2019/03/02/test.json', limit=1)
        assert len(list(generator)) == 1

class TestExtract(unittest.TestCase):

    def test_extract_hb(self):
        extract_date = datetime.strptime('2019-03-02', transform.EXTRACT_DATE_FORMAT).date()
        generator = Extract.extract('test/data/hb', extract_date)
        assert len(list(generator)) == 10

    def test_extract_wwc(self):
        extract_date = datetime.strptime('2019-03-02', transform.EXTRACT_DATE_FORMAT).date()
        generator = Extract.extract('test/data/wwc', extract_date)
        assert len(list(generator)) == 5

    def test_extract_file_not_found(self):
        extract_date = datetime.strptime('2019-03-03', transform.EXTRACT_DATE_FORMAT).date()
        with self.assertRaises(SystemExit):
            Extract.extract('test/data/hb', extract_date)

    def test_extract_dir_not_found(self):
        extract_date = datetime.strptime('2019-03-05', transform.EXTRACT_DATE_FORMAT).date()
        with self.assertRaises(SystemExit):
            Extract.extract('test/data/hb', extract_date)
