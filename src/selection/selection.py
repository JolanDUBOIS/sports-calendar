import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.selection import logger
from src.utils import concatenate_unique_rows


class Selection:
    """ TODO """

    # TODO - Remove if else statement for filter and implement dictionary of functions

    def __init__(self, name: str, items: list[dict], database_path: Path, **kwargs):
        """ TODO """
        self.name = name
        self.items = items
        
        self.read_db(database_path)
    
    def read_db(self, database_path: Path):
        """ TODO """
        self.db_matches = pd.read_csv(database_path / "live_soccer_matches.csv")
        self.db_matches["created_at"] = pd.to_datetime(self.db_matches["created_at"], format="%Y-%m-%d %H:%M:%S")
        self.db_matches = self.db_matches[self.db_matches["created_at"] == self.db_matches["created_at"].max()] # Only keep the most recently updated matches
        self.db_matches = self.db_matches[pd.to_datetime(self.db_matches["Date"], format="%Y-%m-%d").dt.date >= datetime.today().date()] # Only keep future matches
        
        self.db_standings = pd.read_csv(database_path / "live_soccer_standings.csv")
    
    def get_matches(self) -> pd.DataFrame:
        """ TODO """
        matches = pd.DataFrame()
        for item in self.items:
            if item['type'] == 'team':
                logger.info(f"Getting matches for team {item['team_name']}")
                new_matches = self.get_team_matches(item['team_name'], item.get('filter_type'), item.get('filter_spec'))
            elif item['type'] == 'competition':
                logger.info(f"Getting matches for competition {item['competition_name']}")
                new_matches = self.get_competition_matches(item['competition_name'], item.get('filter_type'), item.get('filter_spec'))
            else:
                new_matches = pd.DataFrame()
                logger.error(f"Unknown item type '{item['type']}'")
            matches = concatenate_unique_rows(matches, new_matches, columns_to_compare=['Home Team', 'Away Team', 'Competition', 'Date'])
        return matches

    @classmethod
    def from_json(cls, json_file_path: Path, database_path: Path, **kwargs) -> 'Selection':
        """ TODO - Read a json file instead of taking a dictionary """
        with json_file_path.open(mode='r') as file:
            json_content = json.load(file)
        return Selection(name=json_content['name'], items=json_content['items'], database_path=database_path, **kwargs)

    def get_team_matches(self, team: str, filter: dict|None=None, filter_specification: any=None) -> pd.DataFrame:
        """ TODO """
        all_team_matches = self.db_matches[(self.db_matches["Home Team"] == team) | (self.db_matches["Away Team"] == team)]
        logger.debug("All team matches:")
        logger.debug(all_team_matches['Title'])
        if filter == 'competitions':
            competition = filter_specification
            return all_team_matches[all_team_matches["Competition"] == competition]
        elif filter == 'opponent_ranking':
            logger.warning("Opponent ranking filter not implemented yet.")
        else:
            return all_team_matches
    
    def get_competition_matches(self, competition: str, filter: dict|None=None, filter_specification: any=None) -> pd.DataFrame:
        """ TODO """
        all_competition_matches = self.db_matches[self.db_matches["Competition"] == competition]
        logger.debug("All competition matches:")
        logger.debug(all_competition_matches['Title'])
        if filter == 'teams':
            teams = filter_specification
            return all_competition_matches[all_competition_matches["Home Team"].isin(teams) & all_competition_matches["Away Team"].isin(teams)]
        elif filter == 'min_ranking':
            min_ranking = filter_specification
            competition_standings = self.db_standings[self.db_standings["Competition"] == competition]
            merged_all_competition_matches = pd.merge(all_competition_matches, competition_standings[['Team', 'Position']], how='left', left_on='Home Team', right_on='Team').drop(columns=['Team']).rename(columns={"Position": "position_home"})
            merged_all_competition_matches = pd.merge(merged_all_competition_matches, competition_standings[['Team', 'Position']], how='left', left_on='Away Team', right_on='Team').drop(columns=['Team']).rename(columns={"Position": "position_away"})
            return merged_all_competition_matches[(merged_all_competition_matches["position_home"] <= min_ranking) & (merged_all_competition_matches["position_away"] <= min_ranking)].drop(columns=['position_home', 'position_away'])
        elif filter == 'stage':
            logger.warning("Stage filter not implemented yet.")
        else:
            return all_competition_matches
