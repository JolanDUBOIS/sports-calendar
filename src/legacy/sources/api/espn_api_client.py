from datetime import datetime

import pandas as pd

from .base_api_client import BaseApiClient
from src.legacy.sources import logger


# TODO - Careful with rate limites
class ESPNApiClient(BaseApiClient):
    """ TODO """

    def __init__(self, **kwargs):
        """ TODO """
        super().__init__(**kwargs)

    @property
    def base_url(self) -> str:
        """ Getter for the base_url property. """
        return "https://site.api.espn.com/apis/site/v2/sports/soccer/"

    @property
    def source_name(self):
        """ Getter for the source_name property. """
        return "espn"

    def get_matches(
        self,
        competition_id: str = None,
        team_id: str = None,
        date_from: str = None,
        date_to: str = None,
        **kwargs
    ) -> pd.DataFrame:
        """ TODO """
        competition_slug = self.competition_registry.get_route(self.source_name, competition_id)
        if competition_slug is None:
            # TODO - error or warning msg ? 
            return pd.DataFrame()
        
        # TODO - error msg if team_id
        
        datetime_from = datetime.strptime(date_from, "%Y-%m-%d") if date_from else None
        datetime_to = datetime.strptime(date_to, "%Y-%m-%d") if date_to else None

        datetime_from = datetime_from or datetime.now()
        datetime_to = datetime_to or datetime.now()
        if datetime_from > datetime_to:
            logger.error("date_from is greater than date_to. Returning empty DataFrame.")
            return pd.DataFrame()
        date_range = pd.date_range(datetime_from, datetime_to, freq='D')

        all_matches = []
        for date in date_range:
            date_str = date.strftime("%Y%m%d")
            matches = self._get_matches(competition_slug, date_str)
            all_matches.append(matches)

        return pd.concat(all_matches, ignore_index=True) if all_matches else pd.DataFrame()

    def _get_matches(self, competition_slug: str, date_str: str) -> pd.DataFrame:
        """ TODO - date_str format YYYYMMDD """
        ROOT_PATHS = {
            "competition_source_name": ["leagues", 0, "name"],
            "competition_source_abbreviation": ["leagues", 0, "abbreviation"],
            "season": ["leagues", 0, "season", "year"],
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
        response = self.query_api(url)

        if response is None:
            return pd.DataFrame()

        root_data = {}
        for key, path in ROOT_PATHS.items():
            root_data[key] = self.get_value_from_json(response, path)
        root_data["competition_id"] = self.competition_registry.get_id_by_alias(root_data["competition_source_name"])

        matches = []
        events = response.get('events', [])
        for event in events:
            match = root_data.copy()

            for key, path in EVENT_PATHS.items():
                match[key] = self.get_value_from_json(event, path)

            if match["team_A_home_away"] == "home":
                match["home_team_source_name"] = match["team_A"]
                match["home_team_source_abbreviation"] = match["team_A_abbreviation"]
                match["away_team_source_name"] = match["team_B"]
                match["away_team_source_abbreviation"] = match["team_B_abbreviation"]
            else:
                match["home_team_source_name"] = match["team_B"]
                match["home_team_source_abbreviation"] = match["team_B_abbreviation"]
                match["away_team_source_name"] = match["team_A"]
                match["away_team_source_abbreviation"] = match["team_A_abbreviation"]

            match["home_team_id"] = self.team_registry.get_id_by_alias(match["home_team_source_name"])
            match["away_team_id"] = self.team_registry.get_id_by_alias(match["away_team_source_name"])

            matches.append(match)

        matches_df = pd.DataFrame(matches)
        return matches_df

    def get_standings(self, league_id: str, date: str = None, **kwargs) -> pd.DataFrame:
        """ TODO """
        raise NotImplementedError


if __name__ == "__main__":
    cps = [
        'ligue_1',
        'bundesliga',
        'serie_a',
        'premier_league',
        'la_liga',
        'eredivisie',
        'primeira_liga',
        'eliteserien'
    ]
    
    api_client = ESPNApiClient()
    competitions = api_client.competition_registry.data
    competitions = {k: v for k, v in competitions.items() if k in cps}
    all_matches = []
    for competition_id, competition_data in competitions.items():
        logger.info(f"Fetching matches for competition: {competition_data['name']}")
        matches = api_client.get_matches(competition_id=competition_id, date_to="2025-05-14")
        all_matches.append(matches)
    all_matches_df = pd.concat(all_matches, ignore_index=True)
    all_matches_df.to_csv("~/projects/personal_projects/sports-calendar-project/football-calendar/data/espn/matches.csv", index=False)
