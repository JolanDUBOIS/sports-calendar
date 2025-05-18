import time
import traceback

import requests
import cloudscraper
import pandas as pd
from bs4 import BeautifulSoup

from src.sources import BaseSourceClient, logger


class BaseScraper(BaseSourceClient):
    """ TODO """

    def __init__(self, **kwargs):
        """ TODO """
        super().__init__(**kwargs)

    @property
    def base_url(self) -> str:
        """ Getter for the base_url property. """
        pass

    @property
    def source_name(self):
        """Getter for the source_name property."""
        pass

    def scrape_url(self, url: str, max_retries: int = 3, delay: int = 5) -> BeautifulSoup|None:
        """ TODO """
        retries = 0
        scraper = cloudscraper.create_scraper()
        while retries < max_retries:
            try:
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

            except Exception as e:
                logger.error(f"Failed to fetch data from URL: {url}. Error: {e}")
                logger.debug(traceback.format_exc())
                break

        if retries >= max_retries:
            logger.error(f"Max retries reached for URL: {url}. Failed to fetch data.")

        return None

    def get_matches(
        self,
        competition_id: str = None,
        team_id: str = None,
        date_from: str = None,
        date_to: str = None,
        **kwargs
    ) -> pd.DataFrame:
        """ TODO """
        return pd.DataFrame()

    def get_standings(self, league_id: str, date: str = None, **kwargs) -> dict|None:
        """ TODO """
        return pd.DataFrame()
