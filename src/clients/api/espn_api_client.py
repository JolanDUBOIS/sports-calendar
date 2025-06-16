from datetime import datetime

from . import logger
from .base_api_client import BaseApiClient
from ..utils import generate_date_range


class ESPNApiClient(BaseApiClient):
    """ TODO """

    base_url = "https://site.api.espn.com/apis/v2/sports/soccer/"
    site_base_url = "https://site.api.espn.com/apis/site/v2/sports/soccer/"

    def __init__(self, **kwargs):
        """ TODO """
        super().__init__(**kwargs)

    def query_competitions(self) -> list[dict]:
        """ TODO """
        logger.info("Querying competitions from ESPN API.")
        url = "https://site.api.espn.com/apis/site/v2/leagues/dropdown?lang=en&sport=soccer"
        response = self.query_api(url)
        if not response:
            return []
        competitions = response.get("leagues", [])
        return competitions

    def query_matches(self, competition_slug: str, date_from: str = None, date_to: str = None) -> list[dict]:
        """ TODO """
        logger.info(f"Querying matches for competition: {competition_slug} from {date_from} to {date_to}.")
        date_from = date_from or datetime.now().strftime("%Y-%m-%d")
        date_to = date_to or datetime.now().strftime("%Y-%m-%d")
        url = f"{self.site_base_url}{competition_slug}/scoreboard?dates={self.format_date_range(date_from, date_to)}"
        response = self.query_api(url)
        if not response:
            return []
        leagues = response.get("leagues", [])
        matches = response.get("events", [])
        for event in matches:
            event["leagues"] = leagues
        return matches

    def query_standings(self, competition_slug: str) -> list[dict]:
        """ TODO """
        logger.info(f"Querying standings for competition: {competition_slug}.")
        url = f"{self.base_url}{competition_slug}/standings"
        response = self.query_api(url)
        if not response:
            return []
        else:
            return [response]

    @staticmethod
    def format_date_range(date_from: str, date_to: str) -> str:
        """ TODO """
        try:
            start_date = datetime.strptime(date_from, "%Y-%m-%d")
        except ValueError:
            start_date = datetime.strptime(date_from, "%Y-%m-%d %H:%M:%S")
        try:
            end_date = datetime.strptime(date_to, "%Y-%m-%d")
        except ValueError:
            end_date = datetime.strptime(date_to, "%Y-%m-%d %H:%M:%S")
        return f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
