from datetime import datetime, timezone

from . import logger
from .base_api_client import BaseApiClient
from ..utils import remove_keys as deep_remove_keys


DEFAULT_KEYS_TO_REMOVE = {
    "competitions": ["links", "logos"],
    "scoreboard": ["geoBroadcasts", "broadcasts", "headlines", "odds", "logos", "links"],
    "standings": ["logos", "links"]
}

class ESPNApiClient(BaseApiClient):
    """ TODO """

    BASE_URL = "https://site.api.espn.com/apis"

    def __init__(self, **kwargs):
        """ TODO """
        super().__init__(**kwargs)

    def query_competitions(self, sport: str, trim: bool = False) -> list[dict]:
        """ TODO """
        logger.info("Querying competitions from ESPN API.")
        url = f"{self.BASE_URL}/site/v2/leagues/dropdown?lang=en&sport={sport}"
        response = self.query_api(url)
        if not response:
            return []
        competitions = response.get("leagues", [])
        return deep_remove_keys(competitions, DEFAULT_KEYS_TO_REMOVE["competitions"]) if trim else competitions

    def query_scoreboard(self, sport: str, league: str, date_from: str = None, date_to: str = None, trim: bool = False) -> list[dict]:
        """ TODO """
        logger.info(f"Querying scoreboard for sport {sport} and competition {league} from {date_from[:10]} to {date_to[:10]}.")
        date_from = date_from or datetime.now(timezone.utc).isoformat(timespec="seconds")
        date_to = date_to or datetime.now(timezone.utc).isoformat(timespec="seconds")
        url = f"{self.BASE_URL}/site/v2/sports/{sport}/{league}/scoreboard?dates={self.format_date_range(date_from, date_to)}"
        response = self.query_api(url)
        if not response:
            return []
        leagues = response.get("leagues", [])
        events = response.get("events", [])
        for event in events:
            event["leagues"] = leagues
        return deep_remove_keys(events, DEFAULT_KEYS_TO_REMOVE["scoreboard"]) if trim else events

    def query_standings(self, sport: str, league: str, trim: bool = False) -> list[dict]:
        """ TODO """
        logger.info(f"Querying standings for for sport {sport} and competition {league}.")
        url = f"{self.BASE_URL}/v2/sports/{sport}/{league}/standings"
        response = self.query_api(url)
        if not response:
            return []
        else:
            return deep_remove_keys([response], DEFAULT_KEYS_TO_REMOVE["standings"]) if trim else [response]

    @staticmethod
    def format_date_range(date_from: str, date_to: str) -> str:
        """ TODO """
        try:
            start_date = datetime.fromisoformat(date_from)
        except ValueError:
            logger.error(f"Invalid date format for date_from: {date_from}.")
            raise
        try:
            end_date = datetime.fromisoformat(date_to)
        except ValueError:
            logger.error(f"Invalid date format for date_to: {date_to}.")
            raise
        return f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
