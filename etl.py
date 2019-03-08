import argparse
from datetime import datetime
import logging
import sys
import os

from extract import Extract
import transform
from transform import Transform
import load
from load import Load

logger = logging.getLogger(__name__)

SUPPORTED_GAMES=['hb', 'wwc']
DATA_DIR='data'

def main(args):
    game, extract_date = parse_args(args)
    etl(game, extract_date)

def etl(game, extract_date, data_dir=DATA_DIR, db=load.DB_FILENAME):
    logger.info('Start ETL for game {0}'.format(game))
    load_date = datetime.today()
    data_dir = os.path.join(data_dir, game)
    
    extract_data = Extract.extract(data_dir, extract_date)
    if game == 'hb':
        trans_fun = Transform.hb_transform
    elif game == 'wwc':
        trans_fun = Transform.wwc_transform
    transform_data = Transform.transform(extract_data, trans_fun, extract_date, load_date)
    Load.load(transform_data, game, db=db)

def parse_args(args):
    parser = argparse.ArgumentParser(description='Tiny ETL')
    parser.add_argument('game', help='Name of the game')
    parser.add_argument('date', help='Data extract date in format YYYY-MM-DD')
    args = parser.parse_args(args)

    game = args.game
    if game not in SUPPORTED_GAMES:
        logger.error('ETL for {0} is not supported. Try one of {1}'.format(game, 
                                                                    SUPPORTED_GAMES))
        exit(1)
    try:
        extract_date = datetime.strptime(args.date, transform.EXTRACT_DATE_FORMAT).date()
    except ValueError:
        logger.error('Date not in format YYYY-MM-DD: {0}'.format(args.date))
        exit(1)

    return game, extract_date

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main(sys.argv[1:])