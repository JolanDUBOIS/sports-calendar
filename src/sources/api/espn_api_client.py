import time, traceback
from typing import Any

import requests
import pandas as pd

from src.sources import logger


# TODO - Careful with rate limites
class ESPNApiClient:
    """ TODO """

    def __init__(self):
        """ TODO """

    @property
    def base_url(self) -> str:
        """ TODO """
        return "https://site.api.espn.com/apis/site/v2/sports/soccer/"

    def get_url_response(self, url: str, max_retries: int = 3, delay: int = 5) -> dict|None:
        """ TODO """
        retries = 0
        while retries < max_retries:
            try:
                response = requests.get(url)

                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    logger.warning(f"Rate limit exceeded (status code 429). Attempt {retries}/{max_retries}. Retrying in {delay} seconds...")
                    time.sleep(delay)
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

    def _get_matches(self, competition_slug: str, date_str: str) -> pd.DataFrame:
        """ TODO """
        ROOT_PATHS = {
            "competition_name": ["leagues", 0, "name"],
            "competition_abbreviation": ["leagues", 0, "abbreviation"],
            "season": ["leagues", "season", "year"],
        }

        EVENT_PATHS = {
            "date_time": ["date"],
            "team_A_home_away": ["competitions", 0, "competitors", 0, "homeAway"],
            "team_A": ["competitions", 0, "competitors", 0, "team", "name"],
            "team_B": ["competitions", 0, "competitors", 1, "team", "name"],
            "team_A_abbreviation": ["competitions", 0, "competitors", 0, "team", "abbreviation"],
            "team_B_abbreviation": ["competitions", 0, "competitors", 1, "team", "abbreviation"],
            # "team_B_home_away": ["competitions", 0, "competitors", 1, "homeAway"],
            "leg": ["competitions", 0, "leg", "displayValue"],
            "stage": ["competitions", 0, "series", "title"],
            "venue": ["competitions", 0, "venue", "fullName"],
            "city": ["competitions", 0, "venue", "address", "city"],
            "country": ["competitions", 0, "venue", "address", "country"],
        } # Make it a class attribute ???? 

        url = f"{self.base_url}{competition_slug}/scoreboard?dates={date_str}"
        response = self.get_url_response(url)

        if response is None:
            return pd.DataFrame()

        competition_name = self._get_value_from_json(response, ROOT_PATHS["competition_name"])
        competition_abbreviation = self._get_value_from_json(response, ROOT_PATHS["competition_abbreviation"])
        season = self._get_value_from_json(response, ROOT_PATHS["season"])

        matches = []
        events = response.get('events', [])
        for event in events:
            match = {}
            match["competition_name"] = competition_name
            match["competition_abbreviation"] = competition_abbreviation
            match["season"] = season

            match["date_time"] = self._get_value_from_json(event, EVENT_PATHS["date_time"])

            team_A_home_away = self._get_value_from_json(event, EVENT_PATHS["team_A_home_away"])
            if team_A_home_away == "home":
                match["home_team_name"] = self._get_value_from_json(event, EVENT_PATHS["team_A"])
                match["home_team_abbreviation"] = self._get_value_from_json(event, EVENT_PATHS["team_A_abbreviation"])
                match["away_team_name"] = self._get_value_from_json(event, EVENT_PATHS["team_B"])
                match["away_team_abbreviation"] = self._get_value_from_json(event, EVENT_PATHS["team_B_abbreviation"])
            else:
                match["home_team_name"] = self._get_value_from_json(event, EVENT_PATHS["team_B"])
                match["home_team_abbreviation"] = self._get_value_from_json(event, EVENT_PATHS["team_B_abbreviation"])
                match["away_team_name"] = self._get_value_from_json(event, EVENT_PATHS["team_A"])
                match["away_team_abbreviation"] = self._get_value_from_json(event, EVENT_PATHS["team_A_abbreviation"])

            match["leg"] = self._get_value_from_json(event, EVENT_PATHS["leg"])
            match["stage"] = self._get_value_from_json(event, EVENT_PATHS["stage"])
            match["venue"] = self._get_value_from_json(event, EVENT_PATHS["venue"])
            match["city"] = self._get_value_from_json(event, EVENT_PATHS["city"])
            match["country"] = self._get_value_from_json(event, EVENT_PATHS["country"])

            matches.append(match)

        matches_df = pd.DataFrame(matches)
        return matches_df

    def get_competition(self, competition: str) -> pd.DataFrame:
        """ TODO """
        raise NotImplementedError

    def get_standings(self, competition: str) -> pd.DataFrame:
        """ TODO """
        raise NotImplementedError

    @staticmethod
    def _get_value_from_json(data: dict, path: list[str, int] | None) -> Any | None:
        """ TODO """
        if path is None:
            return None

        value = data.copy()
        for key in path:
            if isinstance(key, str):
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    logger.error(f"Expected a dict at key '{key}', but got {type(value)}.")
                    return None
            elif isinstance(key, int):
                if isinstance(value, list):
                    value = value[key]
                else:
                    logger.error(f"Expected a list at index '{key}', but got {type(value)}.")
                    return None

        return value
