import numpy as np
import pandas as pd

from . import logger


def reshape_matches(data: pd.DataFrame, source_key, **kwargs) -> pd.DataFrame:
    """ Reshape matches data according to the source key. """
    logger.debug(f"Reshaping matches data for source: {source_key}")
    reshape_function_map = {
        "espn_matches": reshape_espn_matches,
    }
    reshape_function = reshape_function_map.get(source_key)

    if not reshape_function:
        logger.debug(f"Unknown source name: {source_key}")
        return data
    
    return reshape_function(data)

def reshape_espn_matches(data: pd.DataFrame) -> pd.DataFrame: # TODO - Test
    """ Reshape ESPN matches dataframe to have separate home and away team attributes. """
    home_is_A = data["team_A_homeAway"] == "home"
    home = np.where(home_is_A, "A", "B")
    away = np.where(home_is_A, "B", "A")

    for attr in [
        "id", "winner", "score", "abbreviation", "displayName",
        "shortDisplayName", "name", "location", "venue_id"
    ]:
        data[f"home_team_{attr}"] = [
            data.at[i, f"team_{h}_{attr}"] for i, h in zip(data.index, home)
        ]
        data[f"away_team_{attr}"] = [
            data.at[i, f"team_{a}_{attr}"] for i, a in zip(data.index, away)
        ]

    data.drop(
        columns=[col for col in data.columns if col.startswith("team_")],
        inplace=True
    )

    return data
