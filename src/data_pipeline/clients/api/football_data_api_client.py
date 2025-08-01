import os
from dateutil import parser

from . import logger
from .base_api_client import BaseApiClient


class FootballDataApiClient(BaseApiClient):
    """ TODO """

    base_url = "https://api.football-data.org/v4"

    def __init__(self, api_token: str = None, **kwargs):
        """ TODO """
        super().__init__(**kwargs)
        self.api_token = api_token if api_token else os.getenv("FOOTBALL_DATA_API_TOKEN")
        if not self.api_token:
            logger.error("API token is not provided. Please provide it as an argument or set it as an environment variable.")
            raise ValueError("API token is not provided. Please provide it as an argument or set it as an environment variable.")

    @property
    def headers(self):
        """ Returns the headers to be used in API calls. """
        return {
            'X-Auth-Token': self.api_token,
            'Content-Type': 'application/json'
        }

    def query_team_matches(self, team_id: int, date_from: str=None, date_to: str=None) -> list[dict]:
        """ TODO """
        logger.info(f"Querying matches for team ID: {team_id} from {date_from} to {date_to}.")
        date_from = self.parse_date(date_from)
        date_to = self.parse_date(date_to)
        url_fragment = f"/teams/{team_id}/matches"
        url = f"{self.base_url}{url_fragment}"
        params = {"dateFrom": date_from, "dateTo": date_to}
        response = self.query_api(url, params=params, headers=self.headers, request_interval=6)
        if not response:
            return []
        return response.get("matches", [])

    def query_competition_matches(self, competition_id: int, date_from: str=None, date_to: str=None) -> list[dict]:
        """ TODO """
        logger.info(f"Querying matches for competition ID: {competition_id} from {date_from} to {date_to}.")
        date_from = self.parse_date(date_from)
        date_to = self.parse_date(date_to)
        url_fragment = f"/competitions/{competition_id}/matches"
        url = f"{self.base_url}{url_fragment}"
        params = {"dateFrom": date_from, "dateTo": date_to}
        response = self.query_api(url, params=params, headers=self.headers, request_interval=6)
        if not response:
            return []
        return response.get("matches", [])

    def query_standings(self, competition_id: int) -> list[dict]:
        """ TODO """
        url_fragment = f"/competitions/{competition_id}/standings"
        url = f"{self.base_url}{url_fragment}"
        response = self.query_api(url, headers=self.headers, request_interval=6)
        if not response:
            return []
        return [response]

    def query_competitions(self) -> list[dict]:
        """ TODO """
        url_fragment = "/competitions"
        url = f"{self.base_url}{url_fragment}"
        response = self.query_api(url, headers=self.headers, request_interval=6)
        if not response:
            return []
        return response.get("competitions", [])

    def query_areas(self) -> list[dict]:
        """ TODO """
        url_fragment = "/areas"
        url = f"{self.base_url}{url_fragment}"
        response = self.query_api(url, headers=self.headers, request_interval=6)
        if not response:
            return []
        return response.get("areas", [])

    def query_teams(self, competition_id: int) -> list[dict]:
        """ TODO """
        url_fragment = f"/competitions/{competition_id}/teams"
        url = f"{self.base_url}{url_fragment}"
        response = self.query_api(url, headers=self.headers, request_interval=6)
        if not response:
            return []
        return response.get("teams", [])

    @staticmethod
    def parse_date(date_str: str) -> str:
        """ TODO """
        if date_str:
            return parser.parse(date_str).strftime("%Y-%m-%d")
        return None
