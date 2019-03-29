import argparse
import logging
import logging.handlers

from etl import get_faculty_positions, get_citations, merge_data
from config import settings

query_url = settings.config['RAB_QUERY_API']
email = settings.config['ADMIN_EMAIL']
passw = settings.config['ADMIN_PASS']
log_file = settings.config['LOG_FILE']
data_dir = settings.config['DATA_DIR']

logger = logging.getLogger('etl')
handler = logging.handlers.TimedRotatingFileHandler(
            log_file, when='d', interval=1, backupCount=7)
formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s')

logger.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger.addHandler(handler)

def main(debug=False, test=False):
    logger.info("BEGIN ETL PROCESS")
    get_faculty_positions.main(query_url, email, passw,
        data_dir, debug=debug, test=test)
    get_citations.main(query_url, email, passw,
        data_dir, debug=debug, test=test)
    merge_data.main(data_dir, debug=debug, test=test)
    logger.info("END ETL PROCESS")

if __name__ == '__main__':
    arg_parse = argparse.ArgumentParser()
    arg_parse.add_argument('-d','--debug', action="store_true")
    arg_parse.add_argument('-t','--test', action="store_true")
    args = arg_parse.parse_args()
    main(debug=args.debug, test=args.test)