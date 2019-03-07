import argparse
from datetime import datetime
import logging

from extract import Extract
from transform import Transform
from load import Load

logger = logging.getLogger(__name__)

def main():
    game, data_date = parse_args()
    etl(game, data_date)

def etl(game, data_date):
    logger.info('Start ETL for game {0}'.format(game))

    extract_data = Extract.extract(game, data_date)
    if game == 'hb':
        trans_fun = Transform.hb_transform
    elif game == 'wwc':
        trans_fun = Transform.wc_transform
    transform_data = Transform.transform(extract_data, trans_fun, data_date)
    Load.create_schema()
    Load.load(transform_data)

def parse_args():
    parser = argparse.ArgumentParser(description='Game ETL')
    parser.add_argument('game')
    parser.add_argument('date')
    args = parser.parse_args()

    game = args.game
    if game not in ['hb', 'wwc']:
        logger.error('No etl for {0}'.format(game))
        exit(1)
    try:
        data_date = datetime.strptime(args.date, '%Y-%m-%d').date()
    except ValueError:
        logger.error('Date not in format YYYY-MM-DD: {0}'.format(args.date))
        exit(1)

    return game, data_date

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()