import pandas as pd

from .processor_base_class import Processor
from src.data_processing import logger


class ExtractTeams(Processor):
    """ Extract teams from matches data """
    
    mapping_source_to_columns = {
        "espn_matches": {
            "team_id": ["home_team_id", "home_team_id"],
            "team_name": ["home_team_name", "home_team_name"],
            "team_abbreviation": ["home_team_abbreviation", "away_team_abbreviation"],
            "team_displayName": ["home_team_displayName", "away_team_displayName"],
            "team_shortDisplayName": ["home_team_shortDisplayName", "away_team_shortDisplayName"]
        },
        "football_data_matches": {
            "team_id": ["homeTeam_id", "awayTeam_id"],
            "team_name": ["homeTeam_name", "awayTeam_name"],
            "team_shortName": ["homeTeam_shortName", "awayTeam_shortName"],
            "team_tla": ["homeTeam_tla", "awayTeam_tla"]
        },
        "live_soccer_matches": {
            "team_name": ["home_team", "away_team"],
        }
    }

    def process(self, data: pd.DataFrame, source_key: str, **kwargs) -> pd.DataFrame:
        """ TODO """
        columns_mapping = self.mapping_source_to_columns.get(source_key)
        self._check_mapping_extract_teams(columns_mapping)
        target_columns = columns_mapping.keys()
        output_data = pd.DataFrame(columns=target_columns)
        for columns in zip(*columns_mapping.values()):
            logger.debug(f"Extracting columns: {columns}")
            if not all(col in data.columns for col in columns):
                logger.error(f"One of the columns {columns} is not present in the data.")
                raise ValueError(f"One of the columns {columns} is not present in the data.")
            new_rows = data[list(columns)].rename(columns=dict(zip(columns, target_columns)))   
            output_data = pd.concat([output_data, new_rows], ignore_index=True)
        if kwargs.get("deduplicate", True):
            output_data = output_data.drop_duplicates()
        return output_data 

    @staticmethod
    def _check_mapping_extract_teams(config: dict):
        """ TODO """
        if not isinstance(config, dict):
            logger.error("Mapping should be a dictionary.")
            raise ValueError("Mapping should be a dictionary.")
        
        n_splits = None
        for key, value in config.items():
            if not isinstance(value, list):
                logger.error(f"Mapping value for {key} should be a list.")
                raise ValueError(f"Mapping value for {key} should be a list.")
            if n_splits is not None and len(value) != n_splits:
                logger.error(f"Mapping values for {key} should have the same length as the other keys.")
                raise ValueError(f"Mapping values for {key} should have the same length as the other keys.")
            n_splits = len(value)
