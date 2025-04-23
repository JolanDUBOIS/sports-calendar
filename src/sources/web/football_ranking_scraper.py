import re, time, traceback, requests

import cloudscraper # type: ignore
import pandas as pd
from bs4 import BeautifulSoup # type: ignore

from src.sources import logger


class FootballRankingScraper:
    """ TODO """

    def __init__(self):
        self.base_url = 'https://football-ranking.com'

    def get_url_response(self, url: str, max_retries=5) -> requests.Response|None:
        """ TODO """
        try:
            scraper = cloudscraper.create_scraper()
            response = scraper.get(url)

            # Retry if rate limit is hit (HTTP 429)
            retries = 0
            while response.status_code == 429 and retries < max_retries:  # Too many requests
                logger.warning(f"Rate limit hit, waiting for {self.wait_time} seconds...")
                time.sleep(self.wait_time)
                response = scraper.get(url)
                retries += 1

            if retries == max_retries:
                logger.error(f"Max retries reached ({max_retries}). Could not fetch URL: {url}")
                return None

            # Handle other error codes
            if response.status_code != 200:
                logger.error(f"Error {response.status_code} while accessing URL: {url}")
                return None

        except Exception as e:
            logger.error(f"Error while scraping: {e}")
            logger.debug(traceback.format_exc())
            return None

        return response

    def get_fifa_rankings(self) -> pd.DataFrame|None:
        """ TODO """
        logger.debug(f"Fetching FIFA ranking...")

        rankings = pd.DataFrame(columns=["Position", "Team", "Points"])
        for k in range(1, 6):
            logger.debug(f"Fetching page {k}...")
            url = f'{self.base_url}/fifa-rankings?page={k}'
            response = self.get_url_response(url)
            if not response:
                continue

            soup = BeautifulSoup(response.text, 'html.parser')

            rankings_list = []
            for row in soup.select("table tbody tr"):
                cols = row.find_all("td")
                if len(cols) < 5:
                    continue

                rank = cols[0].text.strip()
                team = cols[1].text.strip().split("(")[0].strip()  # Remove country code (e.g., "(FRA)")
                points = cols[2].text.strip()
                
                rank = re.match(r'\d+', rank)
                rank = rank.group(0) if rank else ''
                
                points = points.replace(',', '')
                points = re.match(r'\d+(\.\d+)?', points)
                points = points.group(0) if points else ''

                rankings_list.append([rank, team, points])

            rankings = pd.concat([rankings, pd.DataFrame(rankings_list, columns=["Position", "Team", "Points"])], ignore_index=True)

        return rankings
