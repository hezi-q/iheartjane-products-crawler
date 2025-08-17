<<<<<<< HEAD
# iheartjane-products-crawler
=======
# IheartJane Brands and Dispensary Scrapers
Install requirements from requirements.txt file with command:
pip3 install -r requirements.txt

## config.ini
The following parameters can be changed according to requirements.

ZYTE_API_KEY: zyte.com api key to be used to handle proxies.

latitude: latitude of the location you want to scrape data from
longitude: longitude of the location you want to scrape data from

for example, for Colorado, latitude and longitude are 39.739235 and -104.990251 respectively

## Brands Scraper
This scraper scrapes all the brands nearby latitude and longitude given in config.ini file.

run by:
python3 iheartjane_brands_scraper.py

log file: leafly_scraper.log
output file: iheartjane_brands.jsonl

## Dispensary Scraper
This scraper scrapes all the dispensaries nearby latitude and longitude given in config.ini file.

run by:
python3 iheartjane_dispensary_scraper.py

log file: ldispensary_scraper.log
output file: iheartjane_dispensary.jsonl

## Possible Errors
1: Failed to fetch store IDS: Probable cause can be wrong API key.
2: ProxyError: Probable cause can be wrong API key.
