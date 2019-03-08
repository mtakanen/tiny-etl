import urllib.request
import json
from datetime import datetime
import logging

from ip_cache import IPCache

EXTRACT_DATE_FORMAT = '%Y-%m-%d'
HB_DOB_FORMAT = '%m/%d/%Y'
WWC_DOB_FORMAT = '%Y-%m-%d %H:%M:%S'
IPAPI_URL = 'https://ipapi.co/{0}/country_name'
ISO2_COUNTRY_FILENAME = 'res/iso2country.json'
DROP_FIELDS_HB =  ['id','first_name','last_name','email','ip_address', 'dob']
DROP_FIELDS_WWC = ['name', 'location', 'email', 'login', 'registered', 
                   'phone', 'cell', 'picture', 'dob', 'nat', 'id']

logger = logging.getLogger(__name__)
ip_cache = IPCache()

class Transform:

    @staticmethod
    def transform(data, trans_fun, extract_date, load_date):
        logger.info('Transform data')
        data = map(lambda record: trans_fun(record, extract_date, load_date), data)
        return data

    @staticmethod
    def hb_transform(record, extract_date, load_date):
        record['accountid'] = record['id'] 
        record['country'] = ip_to_country_cache(record['ip_address'])
        record['age'] = dob_to_age(record['dob'], HB_DOB_FORMAT, extract_date)
        record['gender'] = record['gender'].lower()
        # add data date and load date
        record['extract_date'] = date_srt(extract_date)
        record['load_date'] = date_srt(load_date)
        # drop redundant fields
        for key in DROP_FIELDS_HB:
            del record[key]
        return record

    @staticmethod
    def wwc_transform(record, extract_date, load_date):
        record['accountid'] = record['login']['salt']
        record['country'] = nat_to_country(record['nat'])
        record['age'] = dob_to_age(record['dob'], WWC_DOB_FORMAT, extract_date)
        record['gender'] = record['gender'].lower()
        # add constant fields
        record['extract_date'] = date_srt(extract_date)
        record['load_date'] = date_srt(load_date)
        # drop redundant fields
        for key in DROP_FIELDS_WWC:
            del record[key]
        return record 

def date_srt(date):
    return date.strftime(EXTRACT_DATE_FORMAT)

def ip_to_country_cache(ip_address, cache=ip_cache):
    country = cache.get(ip_address)
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

def dob_to_age(dob, dob_format, extract_date):
    try:
        dob_date = datetime.strptime(dob, dob_format).date()
        delta = extract_date - dob_date
        age = int(delta.days / 365.25)
    except ValueError as err:
        logger.warning('dob_to_age() failed to parse dob: {0}'.format(err))
        age = None
    return age

def nat_to_country(nat):
    return iso2country.get(nat)

def load_iso2_country(filename):
    iso2country = {} 
    with open(filename) as fp: 
        for item in json.load(fp):
            iso2country[item['Code']] = item['Name']
    return iso2country

iso2country = load_iso2_country(ISO2_COUNTRY_FILENAME)
