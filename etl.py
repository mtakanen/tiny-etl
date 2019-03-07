import argparse
from datetime import datetime
import logging

from extract import Extract
from transform import Transform, EXTRACT_DATE_FORMAT
from load import Load

logger = logging.getLogger(__name__)


def main():
    game, extract_date = parse_args()
    etl(game, extract_date)

def etl(game, extract_date):
    logger.info('Start ETL for game {0}'.format(game))

    extract_data = Extract.extract(game, extract_date)
    if game == 'hb':
        trans_fun = Transform.hb_transform
    elif game == 'wwc':
        trans_fun = Transform.wc_transform
    transform_data = Transform.transform(extract_data, trans_fun, extract_date)
    Load.load(transform_data, game)

def parse_args():
    parser = argparse.ArgumentParser(description='Game account data ETL')
    parser.add_argument('game', help='Name of the game')
    parser.add_argument('date', help='Data extract date in format YYYY-MM-DD')
    args = parser.parse_args()

    game = args.game
    if game not in ['hb', 'wwc']:
        logger.error('No etl for {0}'.format(game))
        exit(1)
    try:
        extract_date = datetime.strptime(args.date, EXTRACT_DATE_FORMAT).date()
    except ValueError:
        logger.error('Date not in format YYYY-MM-DD: {0}'.format(args.date))
        exit(1)

    return game, extract_date

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()