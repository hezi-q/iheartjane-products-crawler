from base64 import b64decode
import configparser
import json
import os
import requests
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup


def config():
    _config = configparser.ConfigParser()
    _config.read('config.ini')

    return _config['settings']


def get(url, _json=False):
    api_key = config()['ZYTE_API_KEY'].strip()

    api_response = requests.post(
        "https://api.zyte.com/v1/extract",
        auth=(api_key, ""),
        json={
            "url": url, 
            "httpResponseBody": True,
        }
    )
    if not api_response.json().get("httpResponseBody"):
        return get(url, _json)
    
    response: bytes = b64decode(
    api_response.json()["httpResponseBody"])
    
    if _json:
        return json.loads(response)
    
    return BeautifulSoup(response, 'html.parser')


def post(url, payload, _json=False):
    api_key = config()['ZYTE_API_KEY'].strip()

    response = requests.post(
        url,
        data=payload,
        proxies={
            "http": f"http://{api_key}:@api.zyte.com:8011/",
            "https": f"http://{api_key}:@api.zyte.com:8011/",
        },
        verify='zyte-ca.crt' 
    )

    if _json:
        return response.json()
    
    return BeautifulSoup(response.text, 'html.parser')


def build_url(base_url, params):
    """
    Constructs a URL by appending query parameters.

    :param base_url: The base URL (e.g., 'https://example.com/search')
    :param params: A dictionary of query parameters (e.g., {'q': 'python', 'page': 2})
    :return: A full URL with encoded query parameters
    """
    return f"{base_url}?{urlencode(params)}" if params else base_url


def append_to_jsonl(filename, new_data):
    with open(f"{filename}.jsonl", 'a') as file:
        file.write(json.dumps(new_data) + '\n')
