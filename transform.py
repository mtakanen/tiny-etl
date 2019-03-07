import urllib.request
import json
from datetime import datetime
import logging

from ip_cache import IPCache

EXTRACT_DATE_FORMAT = '%Y-%m-%d'
HB_DOB_FORMAT = '%m/%d/%Y'
WWC_DOB_FORMAT = '%Y-%m-%d %H:%M:%S'
IPAPI_URL = 'https://ipapi.co/{0}/country_name'

logger = logging.getLogger(__name__)
ip_cache = IPCache()

class Transform:

    @staticmethod
    def transform(data, trans_fun, extract_date):
        logger.info('Transform data')
        data = map(lambda record: trans_fun(record, extract_date), data)
        return data

    @staticmethod
    def hb_transform(record, extract_date):
        record['accountid'] = record['id'] 
        record['country'] = ip_to_country_cache(record['ip_address'])
        record['age'] = dob_to_age(record['dob'], HB_DOB_FORMAT, extract_date)
        record['gender'] = record['gender'].lower()
        # add data date and load date
        record['extract_date'] = extract_date.strftime(EXTRACT_DATE_FORMAT)
        record['load_date'] = datetime.today().strftime(EXTRACT_DATE_FORMAT)
        # drop redundant fields
        drop_fields = ['id','first_name','last_name','email','ip_address', 'dob']
        for key in drop_fields:
            del record[key]
        return record

    @staticmethod
    def wc_transform(record, extract_date):
        record['accountid'] = record['login']['salt']
        record['country'] = nat_to_country(record['nat'])
        record['age'] = dob_to_age(record['dob'], WWC_DOB_FORMAT, extract_date)
        record['gender'] = record['gender'].lower()
        # add constant fields
        record['extract_date'] = extract_date.strftime(EXTRACT_DATE_FORMAT)
        record['load_date'] = datetime.today().strftime(EXTRACT_DATE_FORMAT)
        # drop redundant fields
        drop_fields = ['name', 'location', 'email', 'login', 'registered', 
                       'phone', 'cell', 'picture', 'dob', 'nat']
        for key in drop_fields:
            del record[key]
        return record 

def ip_to_country_cache(ip_address):
    country = ip_cache.get(ip_address)
    if country is None:
        country = ip_to_country_service(ip_address)
        ip_cache.add(ip_address, country)

    return country if country != 'Undefined' else None

def ip_to_country_service(ip_address):
    service_url = IPAPI_URL.format(ip_address)
    try:
        with urllib.request.urlopen(service_url) as response:
            return response.read().decode('utf-8')
    except urllib.error.HTTPError as err:
        logger.error('HTTPError requesting {0}:{1}'.format(service_url, err))
        raise
    except urllib.error.URLError as err:
        logger.error('URLError requesting {0}:{1}'.format(service_url, err))
        exit(1)

def nat_to_country(nat):
    return iso2country[nat]

def dob_to_age(dob, dob_format, extract_date):        
    delta = extract_date - datetime.strptime(dob, dob_format).date()
    return int(delta.days / 365.25)

def load_iso2_country():
    iso2country = {} 
    with open('res/iso2country.json') as fp: 
        for item in json.load(fp):
            iso2country[item['Code']] = item['Name']
    return iso2country

iso2country = load_iso2_country()
