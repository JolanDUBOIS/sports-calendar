import re

import pandas as pd
from bs4 import Tag

from . import logger
from .base_scraper import BaseScraper


class FootballRankingScraper(BaseScraper):
    """ TODO """
    
    base_url = "https://football-ranking.com"

    def __init__(self, **kwargs):
        """ TODO """
        super().__init__(**kwargs)

    def scrape_fifa_rankings(self) -> pd.DataFrame:
        """ TODO """
        logger.info("Scraping FIFA rankings...")
        endpoint = "/fifa-rankings"
        rankings = []
        for k in range(1,6):
            url = f"{self.base_url}{endpoint}?page={k}"
            soup = self.scrape_url(url)
            if not soup:
                logger.debug(f"Failed to fetch FIFA ranking page: {url}")
                continue
            for row in soup.select("table tbody tr"):
                parsed_row = self._parse_fifa_ranking(row)
                if parsed_row:
                    rankings.append(parsed_row)
        
        logger.debug(f"Parsed {len(rankings)} FIFA rankings.")
        return pd.DataFrame(rankings)
    
    def _parse_fifa_ranking(self, row: Tag) -> dict|None:
        """ TODO """
        cols = row.find_all("td")
        if len(cols) < 5:
            return None

        rank = cols[0].text.strip()
        team = cols[1].text.strip()
        points = cols[2].text.strip()

        rank = re.match(r'\d+', rank)
        rank = rank.group(0) if rank else ''

        points = points.replace(',', '')
        points = re.match(r'\d+(\.\d+)?', points)
        points = points.group(0) if points else ''

        return {
            "position": rank,
            "team": team,
            "points": points
        }
