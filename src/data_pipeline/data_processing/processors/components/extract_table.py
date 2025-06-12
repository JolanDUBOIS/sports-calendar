import pandas as pd

from . import logger


EXTRACTION_INSTRUCTIONS = {
    "espn_teams": {
        "extraction_type": "double",
        "columns_mapping": {
            "team_id": ["home_team_id", "away_team_id"],
            "team_name": ["home_team_name", "away_team_name"],
            "team_abbreviation": ["home_team_abbreviation", "away_team_abbreviation"],
            "team_displayName": ["home_team_displayName", "away_team_displayName"],
            "team_shortDisplayName": ["home_team_shortDisplayName", "away_team_shortDisplayName"],
            "source": ["source", "source"],
            "source_type": ["source_type", "source_type"],
        },
    },
    "live_soccer_teams": {
        "extraction_type": "double",
        "columns_mapping": {
            "team_name": ["home_team", "away_team"],
            "source": ["source", "source"],
            "source_type": ["source_type", "source_type"],
        },
    },
    "espn_competitions": {
        "extraction_type": "simple",
        "columns": [
            "competition_id", "competition_name", "competition_abbreviation",
            "competition_midsizeName", "competition_slug", "source", "source_type"
        ],
    },
    "football_data_competitions": {
        "extraction_type": "simple",
        "columns": [
            "competition_id", "competition_name", "competition_code",
            "competition_type", "source", "source_type"
        ],
    },
    "football_data_areas": {
        "extraction_type": "simple",
        "columns": ["area_id", "area_name", "area_code", "source", "source_type"],
    },
    "live_soccer_areas": {
        "extraction_type": "simple",
        "columns": ["area", "source", "source_type"],
    },
}

def extract_table(data: pd.DataFrame, key: str, **kwargs) -> pd.DataFrame:
    """ Extracts data from a DataFrame based on predefined instructions for the given key. """
    if not isinstance(data, pd.DataFrame):
        logger.error("The source should be a pandas DataFrame.")
        raise ValueError("The source should be a pandas DataFrame.")
    instructions = EXTRACTION_INSTRUCTIONS.get(key)
    if instructions is None:
        logger.error(f"No extraction instructions found for key: {key}")
        raise ValueError(f"No extraction instructions found for key: {key}")
    extraction_type = instructions.pop("extraction_type")
    if extraction_type == "simple":
        return _simple_extraction(data, **instructions, **kwargs)
    elif extraction_type == "double":
        return _double_extraction(data, **instructions, **kwargs)

def _simple_extraction(
    data: pd.DataFrame,
    columns: list[str],
    deduplicate: bool = False
) -> pd.DataFrame:
    """ Extract specified columns from the DataFrame, optionally deduplicating. """
    output_data = data[columns]
    if deduplicate:
        logger.debug("Deduplicating data")
        output_data = output_data.drop_duplicates()
    return output_data

def _double_extraction(
    data: pd.DataFrame,
    columns_mapping: dict[str, list[str]],
    deduplicate: bool = False
) -> pd.DataFrame:
    """
    Extracts and renames columns from the DataFrame using a columns mapping.
    Each key in columns_mapping is the target column name, and the value is a list of source column names.
    """
    target_columns = columns_mapping.keys()
    output_data = pd.DataFrame(columns=target_columns)
    for columns in zip(*columns_mapping.values()):
        logger.debug(f"Extracting columns: {columns}")
        if not all(col in data.columns for col in columns):
            logger.error(f"One of the columns {columns} is not present in the data.")
            raise ValueError(f"One of the columns {columns} is not present in the data.")
        new_rows = data[list(columns)].rename(columns=dict(zip(columns, target_columns)))
        output_data = pd.concat([output_data, new_rows], ignore_index=True)
    if deduplicate:
        logger.debug("Deduplicating data")
        output_data = output_data.drop_duplicates()
    return output_data
