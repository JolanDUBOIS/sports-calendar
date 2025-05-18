import re

import pandas as pd

from src.legacy.sources import logger
from .base_scraper import BaseScraper


class FootballRankingScraper(BaseScraper):
    """ TODO """

    def __init__(self, **kwargs):
        """ TODO """
        super().__init__(**kwargs)

    @property
    def base_url(self) -> str:
        """ Getter for the base_url property. """
        return "https://football-ranking.com"

    @property
    def source_name(self):
        """ Getter for the source_name property. """
        return "football-ranking"

    def get_matches(
        self,
        competition_id: str = None,
        team_id: str = None,
        date_from: str = None,
        date_to: str = None,
        **kwargs
    ) -> pd.DataFrame:
        """ TODO """
        logger.warning("FootballRankingScraper does not provide match data. Returning empty DataFrame.")
        return pd.DataFrame()

    def get_standings(self, league_id: str, date: str = None, **kwargs) -> pd.DataFrame:
        """ TODO """
        league_route = self.competition_registry.get_route(self.source_name, league_id)  # Either /fifa-rankings or None
        if not league_route:
            logger.debug(f"Invalid league_id: {league_id} for FootballRankingScraper. Returning empty DataFrame.")
            return pd.DataFrame()

        standings = []
        for k in range(1, 6):
            url = f"{self.base_url}{league_route}?page={k}"
            soup = self.scrape_url(url)
            if not soup:
                return standings

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

                standings.append({
                    "competition_id": league_id,
                    "position": rank,
                    "team_source_name": team,
                    "team_id": self.team_registry.get_id_by_alias(team),
                    "points": points
                })

        standings_df = pd.DataFrame(standings)
        return standings_df
