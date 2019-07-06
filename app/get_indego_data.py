import logging

from rideindego.parse_config import get_config as _get_config
from rideindego.transformer import TripDataCleansing

from rideindego.fetch import DataDownloader, DataLinkHTMLExtractor
from rideindego.fetch import get_page_html as _get_page_html

_CONFIG = _get_config()

logging.basicConfig()
logger = logging.getLogger('get_indego_data')


def main():

    etl_trip_data_config = _CONFIG.get('etl', {}).get('trip_data',{})
    logger.info(f'Using config: {etl_trip_data_config}')

    # Download Raw Data
    source_link = etl_trip_data_config.get('source')
    logger.info(f'Will download from {source_link}')
    page = _get_page_html(source_link).get('data',{})
    page_extractor = DataLinkHTMLExtractor(page)
    links = page_extractor.get_data_links()
    logger.info(f'Extracted links from {source_link}: {links}')

    dld = DataDownloader(links, data_type='zip', config=etl_trip_data_config)
    dld.run()

    logger.info('Cleaning up data')
    cleaner = TripDataCleansing(etl_trip_data_config)
    cleaner.pipeline()
    logger.info('Saved clean data: {}'.format(cleaner.combined_data_path))


if __name__ == "__main__":
    main()
    print('END OF GAME')
