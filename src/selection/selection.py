import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.selection import logger
from src.utils import concatenate_unique_rows
from src.legacy.sources import DatabaseManager


class Selection:
    """ TODO """

    # TODO - Remove if else statement for filter and implement dictionary of functions

    def __init__(self, name: str, items: list[dict], database: DatabaseManager, **kwargs):
        """ TODO """
        self.name = name
        self.items = items
        self.read_database(database)
        self.filters = {
            'competitions': self.filter_competitions,
            'leagues': self.filter_leagues,
            'min_ranking': self.filter_min_ranking,
            'stage': self.filter_stage,
            'teams': self.filter_teams
        }

    def read_database(self, database: DatabaseManager):
        """ TODO """
        self.db_matches = database.query('matches', latest_version=True)
        self.db_matches['Date_to_datetime'] = pd.to_datetime(self.db_matches['Date'], format='%Y-%m-%d')
        self.db_matches = self.db_matches[(self.db_matches['Date_to_datetime'] >= datetime.now()) & (self.db_matches['cancelled'] == False)]
        self.db_matches = self.db_matches.drop(columns=['Date_to_datetime'])

        self.db_standings = database.query('standings', latest_version=True)

    @classmethod
    def from_json(cls, json_file_path: Path, database: DatabaseManager, **kwargs) -> 'Selection':
        """ TODO - Read a json file instead of taking a dictionary """
        with json_file_path.open(mode='r') as file:
            json_content = json.load(file)
        return Selection(name=json_content['name'], items=json_content['items'], database=database, **kwargs)

    def get_matches(self) -> pd.DataFrame:
        """ TODO """
        matches = pd.DataFrame()
        for item in self.items:
            if item['type'] == 'team':
                logger.info(f"Getting matches for team {item['team_name']}")
                # new_matches = self.get_team_matches(item['team_name'], item.get('filter_type'), item.get('filter_spec'))
                new_matches = self.get_team_matches(item['team_name'], item.get('filter'))
            elif item['type'] == 'competition':
                logger.info(f"Getting matches for competition {item['competition_name']}")
                # new_matches = self.get_competition_matches(item['competition_name'], item.get('filter_type'), item.get('filter_spec'))
                new_matches = self.get_competition_matches(item['competition_name'], item.get('filter'))
            else:
                new_matches = pd.DataFrame()
                logger.error(f"Unknown item type '{item['type']}'")
            matches = concatenate_unique_rows(matches, new_matches, columns_to_compare=['Home Team', 'Away Team', 'Competition', 'Date'])
        return matches

    def get_team_matches(self, team: str, filter: dict=None) -> pd.DataFrame:
        """ TODO - new version """
        all_team_matches = self.db_matches[(self.db_matches["Home Team"] == team) | (self.db_matches["Away Team"] == team)]
        if filter is None:
            return all_team_matches
        return self.filters[filter['type']](all_team_matches, **filter)

    def get_competition_matches(self, competition: str, filter: dict=None) -> pd.DataFrame:
        """ TODO - new version """
        all_competition_matches = self.db_matches[self.db_matches["Competition"] == competition]
        if filter is None:
            return all_competition_matches
        return self.filters[filter['type']](all_competition_matches, **filter)

    def filter_competitions(self, matches: pd.DataFrame, competitions: list[str], **kwargs) -> pd.DataFrame:
        """ TODO """
        return matches[matches['Competition'].isin(competitions)]

    def filter_teams(self, matches: pd.DataFrame, rule: str, teams: list[str], reference_team: str=None, **kwargs) -> pd.DataFrame:
        """ TODO """
        return self._apply_team_filter(matches, rule, teams, reference_team)
    
    def filter_leagues(self, matches: pd.DataFrame, rule: str, leagues: list[str], reference_team: str=None, **kwargs) -> pd.DataFrame:
        """ TODO """
        valid_teams = list(self.db_standings[self.db_standings['Competition'].isin(leagues)]['Team'].unique())
        return self._apply_team_filter(matches, rule, valid_teams, reference_team)

    def filter_min_ranking(self, matches: pd.DataFrame, rule: str, competition: str, ranking: int, reference_team: str=None, **kwargs) -> pd.DataFrame:
        """ TODO """
        if competition == 'league':
            league_standings = self.db_standings[self.db_standings['Is League'] == True]   # TODO - Check if this is correct
            valid_teams = list(league_standings[league_standings['Position'] <= ranking]['Team'].unique())
        else:
            competition_standings = self.db_standings[self.db_standings['Competition'] == competition]
            if competition_standings.empty:
                logger.error(f"No standings found for competition '{competition}'")
                raise ValueError(f"No standings found for competition '{competition}'")
            valid_teams = list(competition_standings[competition_standings['Position'] <= ranking]['Team'].unique())
        return self._apply_team_filter(matches, rule, valid_teams, reference_team)

    def filter_stage():
        """ TODO """
        raise NotImplementedError

    def _apply_team_filter(self, matches: pd.DataFrame, rule: str, valid_teams: list[str], reference_team: str=None) -> pd.DataFrame:
        """ TODO """
        if rule == 'both':
            return matches[matches['Home Team'].isin(valid_teams) & matches['Away Team'].isin(valid_teams)]
        elif rule == 'any':
            return matches[matches['Home Team'].isin(valid_teams) | matches['Away Team'].isin(valid_teams)]
        elif rule == 'opponent':
            if reference_team is None:
                logger.error("Reference team not provided in 'min_ranking' filter for opponent rule.")
                raise ValueError("Reference team not provided in 'min_ranking' filter for opponent rule.")
            return matches[((matches['Home Team'] == reference_team) & matches['Away Team'].isin(valid_teams)) | ((matches['Away Team'] == reference_team) & matches['Home Team'].isin(valid_teams))]
        else:
            logger.error(f"Unknown rule '{rule}' in 'min_ranking' filter.")
            raise ValueError(f"Unknown rule '{rule}' in 'min_ranking' filter.")
