from pathlib import Path

import pandas as pd

from src.data_processing import logger
from src.data_processing.utils import read_yml_file


def _check_mapping_extract_entities(config: dict):
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

def extract_parallel_entities(
    sources: dict[str, pd.DataFrame],
    columns_mapping: dict,
    deduplicate: bool = False
) -> pd.DataFrame:
    """ TODO """
    if len(sources) != 1:
        logger.error("Only one source is supported for parallel entity extraction.")
        raise ValueError("Only one source is supported for parallel entity extraction.")
    table = next(iter(sources.values()))
    logger.debug(f"Extracting parallel entities from table with columns: {table.columns}")
    logger.debug(f"Extracting parallel entities from table: {table.head(10)}")
    if not isinstance(table, pd.DataFrame):
        logger.error("The source should be a pandas DataFrame.")
        raise ValueError("The source should be a pandas DataFrame.")

    _check_mapping_extract_entities(columns_mapping)
    target_columns = columns_mapping.keys()
    output_table = pd.DataFrame(columns=target_columns)
    for columns in zip(*columns_mapping.values()):
        logger.debug(f"Extracting columns: {columns}")
        if not all(col in table.columns for col in columns):
            logger.error(f"One of the columns {columns} is not present in the table.")
            raise ValueError(f"One of the columns {columns} is not present in the table.")
        new_rows = table[list(columns)].rename(columns=dict(zip(columns, target_columns)))   
        output_table = pd.concat([output_table, new_rows], ignore_index=True)
    if deduplicate:
        output_table = output_table.drop_duplicates()
    return output_table 
