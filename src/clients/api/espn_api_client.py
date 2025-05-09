from datetime import datetime

from src.clients import logger
from src.clients.utils import generate_date_range
from src.clients.api.base_api_client import BaseApiClient


class ESPNApiClient(BaseApiClient):
    """ TODO """
    
    base_url = "https://site.api.espn.com/apis/site/v2/sports/soccer/"

    def __init__(self, **kwargs):
        """ TODO """
        super().__init__(**kwargs)

    def query_matches(self, competition_slug: str, date_from: str = None, date_to: str = None) -> list[dict]:
        """ TODO """
        if not date_from:
            date_from = datetime.now().strftime("%Y-%m-%d")
        if not date_to:
            date_to = datetime.now().strftime("%Y-%m-%d")
        date_list = generate_date_range(date_from, date_to)
        matches = []
        for date_str in date_list:
            new_matches = self.query_matches_single_day(competition_slug, date_str)
            # logger.debug(f"Matches for {date_str}: {new_matches}")
            matches.extend(new_matches)
        # logger.debug(f"Matches for {competition_slug} from {date_from} to {date_to}: {matches}")
        return matches

    def query_matches_single_day(self, competition_slug: str, date_str: str) -> list[dict]:
        """ TODO """
        date_str = date_str.replace("-", "")
        url = f"{self.base_url}{competition_slug}/scoreboard?dates={date_str}"
        response = self.query_api(url)
        if not response:
            return []
        return response.get('events', [])
