import pandas as pd

from . import logger


UNIQUE_COLUMNS = {
    "espn_matches": ["id"],
    "football_data_matches": ["id"],
    "football_data_standings": ["competition_id", "season_id", "team_id"],
    "football_ranking_fifa_rankings": ["team_name"],
    "espn_teams": ["team_id"],
    "football_data_teams": ["team_id"],
    "live_soccer_teams": ["team_name"],
    "espn_competitions": ["competition_id"],
    "football_data_competitions": ["competition_id"],
    "live_soccer_competitions": ["competition"],
    "football_data_areas": ["area_id"],
    "live_soccer_areas": ["area"],
}

def deduplicate(data: pd.DataFrame, key: str, **kwargs) -> pd.DataFrame:
    """ Deduplicate the DataFrame based on unique columns defined for the key. """
    if not isinstance(data, pd.DataFrame):
        logger.error("The source should be a pandas DataFrame.")
        raise ValueError("The source should be a pandas DataFrame.")
    if key not in UNIQUE_COLUMNS:
        logger.debug(f"Key '{key}' not found in UNIQUE_COLUMNS. Returning original DataFrame.")
        return data
    subset = UNIQUE_COLUMNS[key]
    if not set(subset).issubset(data.columns):
        logger.warning(f"Subset columns {subset} not found in DataFrame columns: {data.columns.tolist()}. Returning original DataFrame.")
        return data
    logger.debug(f"Number of rows before deduplication: {len(data)}")
    data = data.drop_duplicates(subset=subset).reset_index(drop=True)
    logger.debug(f"Number of rows after deduplication: {len(data)}")
    return data
