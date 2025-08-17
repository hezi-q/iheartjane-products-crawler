import logging
from concurrent.futures import ThreadPoolExecutor
from math import ceil
from threading import Lock

import helpers

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("leafly_scraper.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
lock = Lock()

gram_sheet = {
    'half_gram': .5,
    'gram': 1,
    'two_gram': 2,
    'eighth_ounce': 3.5,
    'quarter_ounce': 7,
    'half_ounce': 14,
    'ounce': 28
}


def parse_variants(raw_product, lat, long):
    base_url = f'https://www.iheartjane.com/api/v1/products/{raw_product["objectID"]}/stores?lat={lat}&long={long}&max_search_radius=200'
    try:
        raw_variants = helpers.get(base_url, True)
    except Exception as e:
        logger.error(f"Failed to fetch variants for product {raw_product['objectID']}: {e}")
        return []

    variants = []
    for raw_variant in raw_variants.get('stores', []):
        try:
            listing = {'name': raw_variant['name']}
            metrics = {}

            if raw_variant.get('percent_thc'):
                metrics['thc'] = f"{raw_variant['percent_thc']}%"
            if raw_variant.get('product_percent_cbd'):
                metrics['cbd'] = f"{raw_variant['product_percent_cbd']}%"

            for gram_key, quantity in gram_sheet.items():
                price = raw_variant['menu_product'].get(f'price_{gram_key}')
                if price:
                    variant = {
                        'listing': listing,
                        'unit': 'g',
                        'price': price,
                        'quantity': quantity,
                        'metrics': metrics
                    }
                    variants.append(variant)
        except Exception as e:
            logger.warning(f"Failed to parse variant: {e}")

    return variants


def parse_product(raw_product, brand, lat, long):
    try:
        product = {
            'name': raw_product['name'],
            'categories': {
                'parent_categories': raw_product['category'],
                'sub_categories': raw_product['kind_subtype'],
            },
            'description': raw_product['description'],
            'rating': {
                'score': raw_product['aggregate_rating'],
                'reviews_count': raw_product['review_count']
            },
            'license': {
                'license_type': ' & '.join(raw_product['store_types'])
            },
            'variants': parse_variants(raw_product, lat, long),
            'image_urls': [img['id'] for img in raw_product['photos'] if img.get('id')],
            'brand': brand
        }

        with lock:
            logger.info(f"Parsed product: {product['name']}")
            helpers.append_to_jsonl('iheartjane_brands', product)
    except Exception as e:
        logger.error(f"Failed to parse product {raw_product.get('name', 'Unknown')} - {e}")


def parse_brand(_id, lat, long):
    try:
        raw_brand = helpers.get(f'https://www.iheartjane.com/api/v1/brands/{_id}', True)
    except Exception as e:
        logger.error(f"Failed to fetch brand {_id}: {e}")
        return

    brand_info = raw_brand.get('brand', {})
    brand = {
        'banner_image_url': [img.get('image_url', '') for img in brand_info.get('custom_images', [])],
        'logo_url': brand_info.get('logo_url'),
        'name': brand_info.get('name'),
        'description': brand_info.get('description', '')
    }

    logger.info(f"Processing brand: {brand['name']}")

    payload = f'{{"query":"","filters":"brand_id:{_id}","hitsPerPage":1000 ,"facets":["*"]}}'
    product_url = 'https://search.iheartjane.com/1/indexes/products-production/query'

    try:
        raw_products = helpers.post(product_url, payload, True)
    except Exception as e:
        logger.error(f"Failed to fetch products for brand {_id}: {e}")
        return

    for raw_product in raw_products.get('hits', []):
        parse_product(raw_product, brand, lat, long)


def brand_ids(lat, long):
    try:
        raw_ids = helpers.get(
            f'https://www.iheartjane.com/api/v1/stores/ids_by_shopping_preferences?lat={lat}&long={long}&distance=100&fulfillment_type=pickup&store_type=all',
            True
        )
    except Exception as e:
        logger.error(f"Failed to fetch store IDs: {e}")
        return []

    store_ids = ' OR '.join([f'store_id:{_id}' for _id in raw_ids.get('store_ids', [])])
    filters = f'{store_ids}'

    payload = f'{{"query":"","filters":"({filters}) AND store_specific_product:false","aroundLatLng":"{lat}, {long}","aroundRadius":32187,"attributesToRetrieve":["product_brand_id"],"facets":["product_brand_id","applicable_brand_special_ids"],"facetFilters":["at_visible_store:true"]}}'

    try:
        raw_brands = helpers.post('https://search.iheartjane.com/1/indexes/menu-products-production/query', payload, True)
        return raw_brands.get('facets', {}).get('product_brand_id', {}).keys()
    except Exception as e:
        logger.error(f"Failed to fetch brand IDs: {e}")
        return []


def main():
    _config = helpers.config()

    lat = _config['latitude'].strip()
    long = _config['longitude'].strip()

    logger.info("Starting scraping process...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        for _id in brand_ids(lat, long):
            executor.submit(parse_brand, _id, lat, long)


if __name__ == '__main__':
    main()
