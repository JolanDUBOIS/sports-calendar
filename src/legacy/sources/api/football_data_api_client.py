import os

import pandas as pd

from src.sources import logger
from .base_api_client import BaseApiClient


class FootballDataApiClient(BaseApiClient):
    """ TODO """

    def __init__(self, api_token: str = None, **kwargs):
        """ TODO """
        self.api_token = api_token if api_token else os.getenv("FOOTBALL_DATA_API_TOKEN")
        if not self.api_token:
            logger.error("API token is not provided. Please provide it as an argument or set it as an environment variable.")
            raise ValueError("API token is not provided. Please provide it as an argument or set it as an environment variable.")
        super().__init__(**kwargs)

    @property
    def base_url(self) -> str:
        """ Getter for the base_url property. """
        return "https://api.football-data.org/v4/"

    @property
    def source_name(self):
        """ Getter for the source_name property. """
        return "football-data"

    @property
    def headers(self):
        """Returns the headers to be used in API calls."""
        return {
            'X-Auth-Token': self.api_token,
            'Content-Type': 'application/json'
        }

    def get_matches(
        self,
        competition_id: str = None,
        team_id: str = None,
        date_from: str = None,
        date_to: str = None,
        **kwargs
    ) -> pd.DataFrame:
        """ TODO - add error handling - format date %Y-%m-%d - for now competition_id and team_id can't be set at the same time """
        competition_route = self.competition_registry.get_route(self.source_name, competition_id)
        team_route = self.team_registry.get_route(self.source_name, team_id)

        if competition_id and team_id:
            logger.warning("Both competition_id and team_id are provided. Only competition_id will be used.")
        if competition_id:
            # TODO - Maybe override competition_id to be sure ? 
            return self._get_competition_matches(competition_route, date_from, date_to, **kwargs)
        elif team_id:
            # TODO - Maybe override team_id to be sure ? 
            return self._get_team_matches(team_route, date_from, date_to, **kwargs)
        else:
            logger.error("Neither competition_id nor team_id is provided. Returning empty DataFrame.")
            return pd.DataFrame()

    def _get_competition_matches(
        self,
        competition_route: str,
        date_from: str = None,
        date_to: str = None,
        **kwargs
    ) -> pd.DataFrame:
        """ TODO """
        url = f"{self.base_url}/competitions/{competition_route}/matches"
        params = {"dateFrom": date_from, "dateTo": date_to}
        data = self.query_api(url, params=params, headers=self.headers)
        return self.__extract_matches_data(data)

    def _get_team_matches(
        self,
        team_route: str,
        date_from: str = None,
        date_to: str = None,
        **kwargs
    ) -> pd.DataFrame:
        """ TODO """
        url = f"{self.base_url}/teams/{team_route}/matches"
        params = {"dateFrom": date_from, "dateTo": date_to}
        data = self.query_api(url, params=params, headers=self.headers)
        return self.__extract_matches_data(data)

    def __extract_matches_data(self, data: dict) -> pd.DataFrame:
        """ TODO """
        MATCH_PATHS = {
            "area_source_id": ["area", "id"],
            "area_source_name": ["area", "name"],
            "area_source_code": ["area", "code"],
            "season_id": ["season", "id"],
            "season_start_date": ["season", "startDate"],
            "season_end_date": ["season", "endDate"],
            "season_current_matchday": ["season", "currentMatchday"],
            "match_id": ["id"],
            "date_time": ["utcDate"],
            "stage": ["stage"],
            "competition_source_id": ["competition", "id"],
            "competition_source_name": ["competition", "name"],
            "competition_source_code": ["competition", "code"],
            "home_team_source_id": ["homeTeam", "id"],
            "home_team_source_name": ["homeTeam", "name"],
            "home_team_source_short_name": ["homeTeam", "shortName"],
            "home_team_source_tla": ["homeTeam", "tla"],
            "away_team_source_id": ["awayTeam", "id"],
            "away_team_source_name": ["awayTeam", "name"],
            "away_team_source_short_name": ["awayTeam", "shortName"],
            "away_team_source_tla": ["awayTeam", "tla"],
            "home_team_score": ["score", "fullTime", "home"],
            "away_team_score": ["score", "fullTime", "away"],
        }

        matches = []
        for match in data.get("matches", []):
            match_data = {}

            for key, path in MATCH_PATHS.items():
                match_data[key] = self.get_value_from_json(match, path)

            match_data["home_team_id"] = self.team_registry.get_id_by_alias(match_data["home_team_source_name"])
            match_data["away_team_id"] = self.team_registry.get_id_by_alias(match_data["away_team_source_name"])
            match_data["competition_id"] = self.competition_registry.get_id_by_alias(match_data["competition_source_name"])

            matches.append(match_data)

        matches_df = pd.DataFrame(matches)
        return matches_df # TODO - add logic to ensure the df has the expected columns

    def get_standings(self, league_id: str, date: str = None, **kwargs) -> pd.DataFrame:
        """ TODO - add error handling """
        league_route = self.competition_registry.get_route(self.source_name, league_id)
        url = f"{self.base_url}/competitions/{league_route}/standings"
        data = self.query_api(url, headers=self.headers)
        return self.__extract_standings_data(data)

    def __extract_standings_data(self, data: dict, league_id: str) -> pd.DataFrame:
        """ TODO """
        STANDING_PATHS = {
            "team_source_id": ["team", "id"],
            "team_source_name": ["team", "name"],
            "team_source_short_name": ["team", "shortName"],
            "team_source_tla": ["team", "tla"],
            "position": ["position"],
            "points": ["points"],
            "matches_played": ["playedGames"],
            "wins": ["won"],
            "draws": ["draw"],
            "losses": ["lost"],
            "goals_for": ["goalsFor"],
            "goals_against": ["goalsAgainst"],
            "goal_difference": ["goalDifference"]
        }

        standings = []
        for standing in data.get("standings", []):
            if standing.get("type") != "TOTAL":
                continue
            for team in standing.get("table", []):
                team_data = {}
                
                for key, path in STANDING_PATHS.items():
                    team_data[key] = self.get_value_from_json(team, path)
                
                team_data["team_id"] = self.team_registry.get_id_by_alias(team_data["team_source_name"])
                team_data["competition_id"] = league_id
                
                standings.append(team_data)

        standings_df = pd.DataFrame(standings)
        return standings_df # TODO - add logic to ensure the df has the expected columns


if __name__ == '__main__':
    football_data_api_client = FootballDataApiClient()
    matches = football_data_api_client.get_matches("ligue_1")
    matches.to_csv("~/projects/personal_projects/sports-calendar-project/football-calendar/data/football_data/matches_l1.csv")
