import unittest
from datetime import datetime

from transform import *

class TestTransform(unittest.TestCase):

    def test_dob_to_age_format(self):
        extract_date = datetime.strptime('2019-03-06', '%Y-%m-%d').date()
        dob = '1976-03-01 10:15:00'
        dob_format = '%Y-%m-%d %H:%M:%S'
        age = dob_to_age(dob, dob_format, extract_date)
        assert age == 43

        dob = '1976-03-01'
        dob_format = '%Y-%m-%d'
        age = dob_to_age(dob, dob_format, extract_date)
        assert age == 43

    def test_dob_to_age_accuracy(self):
        extract_date = datetime.strptime('2019-03-06', '%Y-%m-%d').date()
        dob = '1976-03-07 13:15:00'
        age = dob_to_age(dob, '%Y-%m-%d %H:%M:%S', extract_date)
        assert age == 42

        extract_date = datetime.strptime('2019-03-06', '%Y-%m-%d').date()
        dob = '1976-03-05 10:00:00'
        age = dob_to_age(dob, '%Y-%m-%d %H:%M:%S', extract_date)
        assert age == 43

                        
