from abc import ABC, abstractmethod

import pandas as pd

from src.deprecated.models_deprecated import logger
from src.deprecated.football_data_source_deprecated import FootballDataConnector


class Item(ABC):
    """ TODO """

    def __init__(self, filter_type: str, filter_spec: any, api_connector: FootballDataConnector, **kwargs):
        """ TODO """
        self.filter_type = filter_type
        self.filter_spec = filter_spec
        self.api_connector = api_connector
    
    @abstractmethod
    def get_matches(self):
        """ TODO """
        raise NotImplementedError
    
    @classmethod
    def from_json(cls, json: dict, api_connector: FootballDataConnector) -> 'Item':
        """ TODO """
        item_type = json.get("type")
        if item_type == "team":
            return TeamItem.from_json(json, api_connector=api_connector)
        elif item_type == "competition":
            return CompetitionItem.from_json(json, api_connector=api_connector)
        else:
            raise ValueError(f"Unknown type '{item_type}' in JSON")

class TeamItem(Item):
    """ TODO """

    def __init__(self, team_id: str, **kwargs):
        """ TODO """
        super().__init__(**kwargs)
        self.team_id = team_id
    
    def get_matches(self) -> pd.DataFrame:
        """ TODO """
        matches = self.api_connector.request_upcoming_team_matches(self.team_id)
        if self.filter_type is None:
            return matches
        elif self.filter_type == 'competitions':
            return self.__get_matches_filter_competitions(matches)
        elif self.filter_type == 'opponent_ranking':
            return self.__get_matches_filter_opponent_ranking(matches)
        else:
            raise ValueError(f"Unknown filter type '{self.filter_type}'")
    
    def __get_matches_filter_competitions(self, matches: pd.DataFrame) -> pd.DataFrame:
        """ TODO """
        matches = matches[matches["competition_id"].isin(self.filter_spec)]
        return matches

    def __get_matches_filter_opponent_ranking(self, matches: pd.DataFrame) -> pd.DataFrame:
        """ TODO """
        raise NotImplementedError("Not implemented yet")

    @classmethod
    def from_json(cls, json: dict, api_connector: FootballDataConnector) -> 'TeamItem':
        """ TODO """
        return cls(
            team_id=json['team_id'],
            filter_type=json.get('filter_type'),
            filter_spec=json.get('filter_spec'),
            api_connector=api_connector
        )

class CompetitionItem(Item):
    """ TODO """

    def __init__(self, competition_id: str, **kwargs):
        """ TODO """
        super().__init__(**kwargs)
        self.competition_id = competition_id
        self._competition_standings = None
    
    @property
    def competition_standings(self) -> pd.DataFrame:
        """ TODO """
        if self._competition_standings is None:
            self._competition_standings = self.api_connector.request_competition_standings(self.competition_id)
            logger.debug(f"Competition standings for competition {self.competition_id}:\n{self._competition_standings}")
        return self._competition_standings
    
    def get_matches(self) -> pd.DataFrame:
        """ TODO """
        matches = self.api_connector.request_upcoming_competition_matches(self.competition_id)
        if self.filter_type is None:
            return matches
        elif self.filter_type == 'teams':
            return self.__get_matches_filter_teams(matches)
        elif self.filter_type == 'min_ranking':
            logger.debug("Min ranking filter")
            return self.__get_matches_filter_min_ranking(matches)
        elif self.filter_type == 'stage':
            return self.__get_matches_filter_stage(matches)
        else:
            raise ValueError(f"Unknown filter type '{self.filter_type}'")
    
    def __get_matches_filter_teams(self, matches: pd.DataFrame) -> pd.DataFrame:
        """ TODO """
        matches = matches[matches["home_team_id"].isin(self.filter_spec) & matches["away_team_id"].isin(self.filter_spec)]
        return matches

    def __get_matches_filter_min_ranking(self, matches: pd.DataFrame) -> pd.DataFrame:
        """ TODO """
        matches = matches.merge(
            self.competition_standings[['team_id', 'position']],
            left_on="home_team_id",
            right_on="team_id",
            suffixes=("", "_home")
        ).rename(columns={"position": "position_home"})
        matches = matches.merge(
            self.competition_standings[['team_id', 'position']],
            left_on="away_team_id",
            right_on="team_id",
            suffixes=("", "_away")
        ).rename(columns={"position": "position_away"})
        logger.debug(f"Old matches: {matches}")
        logger.debug(f"Filter spec: {self.filter_spec}")
        matches = matches[(matches["position_home"] <= int(self.filter_spec)) & (matches["position_away"] <= int(self.filter_spec))]
        logger.debug(f"New matches: {matches}")
        return matches        

    def __get_matches_filter_stage(self, matches: pd.DataFrame) -> pd.DataFrame:
        """ TODO """
        matches = matches[matches["stage"].isin(self.filter_spec)]
        return matches

    @classmethod
    def from_json(cls, json: dict, api_connector: FootballDataConnector) -> 'CompetitionItem':
        """ TODO """
        return cls(
            competition_id=json['competition_id'],
            filter_type=json.get('filter_type'),
            filter_spec=json.get('filter_spec'),
            api_connector=api_connector
        )
