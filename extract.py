import os
import csv
import json
import logging

logger = logging.getLogger(__name__)

class Extract:
    '''Extracts data from local files. Supported data formats are csv and (lines of) json.
    Data is available to the user via a generator object.
    '''
    @staticmethod
    def extract(data_dir, extract_date):
        data_dir = os.path.join(data_dir, date_to_dir(extract_date))
        try:
            files = os.listdir(data_dir)
            if len(files) == 0:
                raise FileNotFoundError
        except FileNotFoundError:
            logger.error('No data for date: {0}'.format(extract_date))
            exit(1)

        # assume one file per directory
        filename = files[0]
        filepath = os.path.join(data_dir, filename)

        logger.info('Read from {0}'.format(filepath))
        if filename.endswith('.csv'):
            generator = read_csv(filepath, limit=0)
        elif filename.endswith('.json'):
            generator = read_json(filepath, limit=0)
        return generator


def read_csv(filename, limit=0):
    '''Reads csv from file. Assumes that file exists

    Args:
        filename
        limit 
    Returns: 
        generator of OrderedDict
    '''
    with open(filename) as fp:
        cnt = 0
        reader = csv.DictReader(fp)
        for row in reader: 
            yield row
            cnt+=1
            if limit != 0 and cnt >= limit:
                break

def read_json(filename, limit=0):
    '''Reads lines of json objects from file. Assumes that file exists

    Args:
        filename
        limit 
    Returns: 
        generator of OrderedDict
    '''

    with open(filename) as fp:
        cnt = 1
        line = fp.readline()
        while line:
            yield json.loads(line)
            line = fp.readline()
            cnt +=1
            if limit > 0 and cnt >= limit:
                break

def date_to_dir(extract_date):
    return os.path.join('{0}'.format(extract_date.year),
                        '{:02}'.format(extract_date.month),
                        '{:02}'.format(extract_date.day))

def validate():
    #TODO validate extracted data
    pass
