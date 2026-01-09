import time

import cloudscraper
from bs4 import BeautifulSoup

from . import logger

class BaseScraper:
    """ TODO """

    def __init__(self, **kwargs):
        """ TODO """

    def scrape_url(self, url: str, max_retries: int = 3, delay: int = 15, request_interval: int = 0) -> BeautifulSoup|None:
        """ TODO """
        retries = 0
        scraper = cloudscraper.create_scraper()
        while retries < max_retries:
            try:
                time.sleep(request_interval)
                response = scraper.get(url)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    return soup
                elif response.status_code == 429:
                    logger.warning(f"Rate limit exceeded (status code 429). Attempt {retries}/{max_retries}. Retrying in {delay} seconds...")
                    time.sleep(delay)
                    retries += 1
                else:
                    logger.error(f"Failed to fetch data from URL: {url}. Status code: {response.status_code}")
                    return None

            except Exception:
                logger.exception(f"Failed to fetch data from URL: {url}. Attempt {retries}/{max_retries}. Retrying in {delay} seconds...")
                break

        if retries >= max_retries:
            logger.error(f"Max retries reached for URL: {url}. Failed to fetch data.")

        return None
