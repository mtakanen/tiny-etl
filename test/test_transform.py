import os
import unittest
from datetime import datetime
from collections import OrderedDict
import logging

import transform
from transform import Transform
from ipdb import IPDb

TEST_DB = 'test/test_ip.db'

logging.disable(logging.CRITICAL)

class TestTransform(unittest.TestCase):

    def test_hb_transform(self):
        record = OrderedDict([('id', '1'), ('first_name', 'Maria'), ('last_name', 'Russell'), 
                              ('email', 'mrussell0@soup.io'), ('gender', 'Female'), 
                              ('ip_address', '141.48.134.32'), ('dob', '5/26/1976')])
        expected = OrderedDict([('gender', 'female'), ('accountid', 'mrussell0@soup.io'), ('country', 'Germany'), 
                                ('age', 42), ('game', 'hb'), ('extract_date', '2019-03-01'), ('load_date', '2019-03-07')])

        extract_date = datetime.strptime('2019-03-01', transform.EXTRACT_DATE_FORMAT).date()
        load_date = datetime.strptime('2019-03-07', transform.EXTRACT_DATE_FORMAT).date()
        result = Transform.hb_transform(record, extract_date, load_date)
        self.assertEqual(result, expected)

    def test_wwc_transform(self):
        record = {'gender': 'female', 'name': {'title': 'ms', 'first': 'amandine', 'last': 'dupont'}, 
                  'location': {'street': '1886 avenue goerges clémenceau', 'city': 'saint-étienne', 
                  'state': 'haute-marne', 'postcode': 50177}, 
                  'email': 'amandine.dupont@example.com', 'login': {'username': 'blackdog395', 
                  'password': 'redrum', 'salt': 'U9LROYgO', 'md5': '0839048a03692bab539e9768da8b6294', 
                  'sha1': 'ee78eb3042f53cfb76457dfed63dfb1b254b6e74', 
                  'sha256': 'ce44a980252f85d36f2864043c9dea5c680aca8c2b491fa0d68b2eb5cc161c55'}, 
                  'dob': '1945-12-15 07:32:34', 'registered': '2013-11-25 21:01:13', 
                  'phone': '01-50-21-29-69', 'cell': '06-51-67-95-81', 
                  'id': {'name': 'INSEE', 'value': '2451152147269 96'}, 
                  'picture': {'large': 'https://randomuser.me/api/portraits/women/66.jpg', 
                  'medium': 'https://randomuser.me/api/portraits/med/women/66.jpg', 
                  'thumbnail': 'https://randomuser.me/api/portraits/thumb/women/66.jpg'}, 
                  'nat': 'FR'}
        expected = {'gender': 'female', 'accountid': 'blackdog395', 'country': 'France', 'age': 73, 
                    'game': 'wwc', 'extract_date': '2019-03-01', 'load_date': '2019-03-07'}

        extract_date = datetime.strptime('2019-03-01', transform.EXTRACT_DATE_FORMAT).date()
        load_date = datetime.strptime('2019-03-07', transform.EXTRACT_DATE_FORMAT).date()
        result = Transform.wwc_transform(record, extract_date, load_date)
        self.assertEqual(result, expected)

    def test_transform(self):
        def trans_fun(record, extract_date, load_date):
            record['baz'] = record['foo'] + 1
            del record['bar']
            return record

        data = [{'foo' : 1, 'bar': 2}, {'foo' : 2, 'bar': 3}]
        expected = [{'foo': 1, 'baz': 2}, {'foo': 2,'baz': 3}]

        result = Transform.transform(data, trans_fun, 'ext_date', 'load_date')
        self.assertEqual(list(result), expected)

class TestTransformUtils(unittest.TestCase):

    def test_dob_to_age_format(self):
        extract_date = datetime.strptime('2019-03-06', '%Y-%m-%d').date()
        dob = '1976-03-01 10:15:00'
        dob_format = '%Y-%m-%d %H:%M:%S'
        age = transform.dob_to_age(dob, dob_format, extract_date)
        assert age == 43

        dob = '1976-03-01'
        dob_format = '%Y-%m-%d'
        age = transform.dob_to_age(dob, dob_format, extract_date)
        assert age == 43

    def test_dob_to_age_format_fails(self):
        extract_date = datetime.strptime('2019-03-06', '%Y-%m-%d').date()
        dob = '19760301'
        dob_format = '%Y-%m-%d %H:%M:%S'
        age = transform.dob_to_age(dob, dob_format, extract_date)
        self.assertIsNone(age)

    def test_dob_to_age_accuracy(self):
        extract_date = datetime.strptime('2019-03-06', '%Y-%m-%d').date()
        dob = '1976-03-07 13:15:00'
        age = transform.dob_to_age(dob, '%Y-%m-%d %H:%M:%S', extract_date)
        assert age == 42

        extract_date = datetime.strptime('2019-03-06', '%Y-%m-%d').date()
        dob = '1976-03-05 10:00:00'
        age = transform.dob_to_age(dob, '%Y-%m-%d %H:%M:%S', extract_date)
        assert age == 43

    def test_load_iso2_to_country(self):
        iso2_country = transform.load_iso2_country(transform.ISO2_COUNTRY_FILENAME)
        assert len(iso2_country.items()) == 249
        assert 'FI' in iso2_country.keys()
        assert 'Finland' in iso2_country.values() 

    def test_nat_to_country(self):
        assert transform.nat_to_country('FI') == 'Finland'

    def test_nat_to_country_none(self):
        self.assertIsNone(transform.nat_to_country('FOO'))

    def test_ip_to_country_service(self):
        country = transform.ip_to_country_service('188.238.114.229')
        assert country == 'Finland'

    def test_date_to_str(self):
        date_str = transform.date_to_srt(datetime.strptime('2019-03-06', '%Y-%m-%d').date())
        assert date_str == '2019-03-06'

class TestTransformIPToCountry(unittest.TestCase):
    @classmethod
    def tearDown(cls):
        try:
            os.remove(TEST_DB)
        except FileNotFoundError:
            pass

    def test_ip_to_country(self):
        db = IPDb(TEST_DB)
        db.add('???.???.???.?', 'Utopia') 

        country = transform.ip_to_country('???.???.???.?', cache=db)
        assert country == 'Utopia'

    def test_ip_to_country_not_cached(self):
        db = IPDb(TEST_DB)
        country = transform.ip_to_country('188.238.114.229', cache=db)
        assert country == 'Finland'