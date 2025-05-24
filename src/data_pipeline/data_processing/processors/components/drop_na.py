import pandas as pd

from . import logger


NON_NULLABLE_COLUMNS = {
    "espn_matches": ["id"],
    "football_data_matches": ["id"],
    "football_data_standings": ["competition_id", "season_id", "team_id"],
    "football_ranking_fifa_rankings": ["team_name"],
    "live_soccer_matches": ["home_team", "away_team"],
    "live_soccer_standings": ["team_source_name"],
    "espn_teams": ["team_id", "team_name"],
    "football_data_teams": ["team_id", "team_name"],
    "live_soccer_teams": ["team_name"],
    "espn_competitions": ["competition_id", "competition_name"],
    "football_data_competitions": ["competition_id", "competition_name"],
    "live_soccer_competitions": ["competition"],
    "football_data_areas": ["area_id", "area_name"],
    "live_soccer_areas": ["area"],
    "team_registry": ["idA", "idB", "sourceA", "sourceB", "_similarity_score"],
    "competition_registry": ["idA", "idB", "sourceA", "sourceB", "_similarity_score"],
    "area_registry": ["idA", "idB", "sourceA", "sourceB", "_similarity_score"],
}

def drop_na(data: pd.DataFrame, key: str, **kwargs) -> pd.DataFrame:
    """ Drop rows with NaN values in the DataFrame. """
    if not isinstance(data, pd.DataFrame):
        logger.error("The source should be a pandas DataFrame.")
        raise ValueError("The source should be a pandas DataFrame.")

    if key not in NON_NULLABLE_COLUMNS:
        logger.debug(f"Key '{key}' not found in NON_NULLABLE_COLUMNS. Returning original DataFrame.")
        return data
    subset = NON_NULLABLE_COLUMNS[key]
    if not set(subset).issubset(data.columns):
        logger.warning(f"Subset columns {subset} not found in DataFrame columns: {data.columns.tolist()}. Returning original DataFrame.")
        return data
    logger.debug(f"Number of rows before dropping NaN values: {len(data)}")
    data = data.dropna(subset=subset).reset_index(drop=True)
    logger.debug(f"Number of rows after dropping NaN values: {len(data)}")
    return data
