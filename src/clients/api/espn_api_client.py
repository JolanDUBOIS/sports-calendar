from datetime import datetime

from . import logger
from .base_api_client import BaseApiClient
from ..utils import generate_date_range


class ESPNApiClient(BaseApiClient):
    """ TODO """
    
    base_url = "https://site.api.espn.com/apis/site/v2/sports/soccer/"

    def __init__(self, **kwargs):
        """ TODO """
        super().__init__(**kwargs)

    def query_matches(self, competition_slug: str, date_from: str = None, date_to: str = None) -> list[dict]:
        """ TODO """
        logger.info(f"Querying matches for competition: {competition_slug} from {date_from} to {date_to}.")
        if not date_from:
            date_from = datetime.now().strftime("%Y-%m-%d")
        if not date_to:
            date_to = datetime.now().strftime("%Y-%m-%d")
        date_list = generate_date_range(date_from, date_to)
        matches = []
        for date_str in date_list:
            new_matches = self.query_matches_single_day(competition_slug, date_str)
            leagues = new_matches.get("leagues", [])
            events = new_matches.get("events", [])
            for event in events:
                event["leagues"] = leagues
            matches.extend(events)
        return matches

    def query_matches_single_day(self, competition_slug: str, date_str: str) -> dict:
        """ TODO """
        date_str = date_str.replace("-", "")
        url = f"{self.base_url}{competition_slug}/scoreboard?dates={date_str}"
        response = self.query_api(url)
        if not response:
            return []
        return response
