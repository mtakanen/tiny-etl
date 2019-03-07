
import json
import urllib.request
from datetime import datetime
import logging

from ip_cache import IPCountryCache

logger = logging.getLogger(__name__)

ip_cache = IPCountryCache()

class Transform:

    @staticmethod
    def transform(data, trans_fun, data_date):
        logger.info('Transform data')
        data = map(lambda record: trans_fun(record, data_date), data)
        ip_cache.persist()
        return data

    @staticmethod
    def hb_transform(record, data_date):
        drop_fields = ['first_name','last_name','email','ip_address','dob']

        record['country'] = ip_to_country_cache(record['ip_address'])
        record['age'] = dob_to_age(record['dob'], '%m/%d/%Y', data_date)
        record['gender'] = record['gender'].lower()
        # add constant fields
        record['game'] = 'hb'
        record['data_date'] = data_date.strftime('%Y-%m-%d')
        record['load_date'] = datetime.today().strftime('%Y-%m-%d')
        # drop redundant fields
        for key in drop_fields:
            del record[key]
        # TODO: show progress
        return record

    @staticmethod
    def wc_transform(record, data_date):
        drop_fields = ['name', 'location', 'email', 'login', 'registered', 'phone',
                        'cell', 'picture', 'dob', 'nat']

        record['id'] = record['login']['salt']
        record['country'] = nat_to_country(record['nat'])
        record['age'] = dob_to_age(record['dob'], '%Y-%m-%d %H:%M:%S', data_date)
        record['gender'] = record['gender'].lower()
        # add constant fields
        record['game'] = 'wwc'
        record['data_date'] = data_date.strftime('%Y-%m-%d')
        record['load_date'] = datetime.today().strftime('%Y-%m-%d')
        # drop redundant fields
        for key in drop_fields:
            del record[key]
        return record 

def ip_to_country_cache(ip_address):
    country = ip_cache.get(ip_address)
    if country is None:
        try:
            country = ip_to_country_service(ip_address)
            ip_cache.add(ip_address, country)
        except urllib.error.HTTPError:
            ip_cache.persist()
            exit(1)

    return country if country != 'Undefined' else None

def ip_to_country_service(ip_address):
    api_url = 'https://ipapi.co/{0}/country_name'.format(ip_address)
    try:
        with urllib.request.urlopen(api_url) as response:
            return response.read().decode('utf-8')
    except urllib.error.HTTPError:
        logger.error('HTTPError requesting {0}'.format(api_url))
        raise
    except urllib.error.URLError:
        logger.error('URLError requesting {0}'.format(api_url))
        exit(1)

def nat_to_country(nat):
    return iso2country[nat]

def dob_to_age(dob, dob_format, data_date):        
    delta = data_date - datetime.strptime(dob, dob_format).date()
    return int(delta.days / 365.25)

def load_iso2_country():
    iso2country = {} 
    with open('res/iso2country.json') as fp: 
        for item in json.load(fp):
            iso2country[item['Code']] = item['Name']
    return iso2country

iso2country = load_iso2_country()
