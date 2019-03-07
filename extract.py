import os
import csv
import json
import logging

logger = logging.getLogger(__name__)

class Extract:
    
    @staticmethod
    def extract(game, data_date):
        data_dir = os.path.join('data', game, date_to_dir(data_date))
        try:
            files = os.listdir(data_dir)
            if len(files) == 0:
                raise FileNotFoundError
        except FileNotFoundError:
            logger.error('No data for date: {0}'.format(data_date))
            exit(1)

        # assume one file per directory
        filename = files[0]
        filepath = os.path.join(data_dir, filename)

        logger.info('Read from {0}'.format(filepath))
        if filename.endswith('.csv'):
            data = read_csv(filepath, limit=0)
        elif filename.endswith('.json'):
            data = read_json(filepath, limit=0)
        return data

    @staticmethod
    def validate():
        fields = ['id', 'first_name', 'last_name', 'email', 'gender', 'ip_address', 'dob']
        #print(self.data[0].keys())
        #if dob_format not in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S']:
        #    raise ValueError


def read_csv(filename, limit=0):
    with open(filename) as fp:
        cnt = 0
        reader = csv.DictReader(fp)
        for row in reader: 
            yield row
            cnt+=1
            if limit != 0 and cnt >= limit:
                break

def read_json(filename, limit=0):
    with open(filename) as fp:
        cnt = 1
        line = fp.readline()
        while line:
            yield json.loads(line)
            line = fp.readline()
            cnt +=1
            if limit > 0 and cnt >= limit:
                break

def date_to_dir(data_date):
    return os.path.join('{0}'.format(data_date.year),
                        '{:02}'.format(data_date.month),
                        '{:02}'.format(data_date.day))