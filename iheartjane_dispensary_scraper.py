import logging
from math import ceil
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import re

import helpers


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("dispensary_scraper.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

lock = Lock()


def parse_brands(store_id):
    try:
        payload = '{"query":"","filters":"store_id =' + store_id + '","facets":["*"]}'
        url = 'https://search.iheartjane.com/1/indexes/menu-products-production/query'

        raw_brands = helpers.post(url, payload, True)

        return list(raw_brands['facets']['brand'].keys())
    except Exception as e:
        logger.error(f"Error fetching brands for store {store_id}: {e}")
        return []

def parse_dispensary(raw_dispensary):
    try:
        dispensary = {}

        dispensary['logo_url'] = raw_dispensary.get('photo', '')
        dispensary['name'] = raw_dispensary['name']

        dispensary['dispensary'] = {}
        dispensary['dispensary']['address'] = raw_dispensary.get('full_address', '')
        dispensary['dispensary']['state'] = raw_dispensary.get('state', '')
        dispensary['dispensary']['city'] = raw_dispensary.get('city', '')
        dispensary['dispensary']['zip_code'] = raw_dispensary.get('full_address', '').split(',')[-1].strip()

        dispensary['contact'] = {
            'phone_number': raw_dispensary.get('phone', '')
        }

        dispensary['description'] = raw_dispensary.get('description', '')

        dispensary['rating'] = {}
        try:
            dispensary['rating']['score'] = round(raw_dispensary['rating'], 2)
        except:
            dispensary['rating']['score'] = None
        dispensary['rating']['reviews_count'] = raw_dispensary.get('reviews_count', '')

        dispensary['license'] = {}
        if raw_dispensary.get('medical') and raw_dispensary.get('recreational'):
            dispensary['license']['license_type'] = 'Recreational and Medical'
        elif raw_dispensary.get('medical'):
            dispensary['license']['license_type'] = 'Medical'
        elif raw_dispensary.get('recreational'):
            dispensary['license']['license_type'] = 'Recreational'

        dispensary['brands'] = parse_brands(raw_dispensary.get('objectID', ''))

        with lock:
            logger.info(f"Parsed dispensary: {dispensary['name']}")
            helpers.append_to_jsonl('iheartjane_dispensary', dispensary)

    except Exception as e:
        logger.error(f"Error parsing dispensary {raw_dispensary.get('name', 'unknown')}: {e}")

def dispensary_scraper():
    logger.info("Starting dispensary scraper")
    
    _config = helpers.config()

    lat = _config['latitude'].strip()
    long = _config['longitude'].strip()

    lat_long =  f'"{lat}, {long}"'
    payload = '{"query":"","filters":"marketplace_visible:true","hitsPerPage":10000,"aroundLatLng":' + lat_long + ',"aroundRadius":507913,"facets":["*"]}'

    base_url = 'https://search.iheartjane.com/1/indexes/stores-production/query'

    try:
        raw_dispensaries = helpers.post(base_url, payload, True)
        logger.info(f"Found {len(raw_dispensaries['hits'])} dispensaries")

        with ThreadPoolExecutor(max_workers=15) as executor:
            for raw_dispensary in raw_dispensaries['hits']:
                executor.submit(parse_dispensary, raw_dispensary)

    except Exception as e:
        logger.error(f"Error in dispensary scraper: {e}")

    
if __name__ == '__main__':
    dispensary_scraper()
