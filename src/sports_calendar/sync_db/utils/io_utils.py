from typing import Any

import pandas as pd

from . import logger
from sports_calendar.sc_core import IOContent


def get_max_field_value(data: IOContent, field: str) -> Any:
    """ Get the maximum value of a field in the data. """
    if isinstance(data, pd.DataFrame):
        return _get_max_field_value_df(data, field)
    elif isinstance(data, list):
        return _get_max_field_value_json(data, field)
    else:
        logger.error(f"Unsupported data type: {type(data)}. Expected pd.DataFrame or list.")
        raise TypeError(f"Unsupported data type: {type(data)}. Expected pd.DataFrame or list.")

def _get_max_field_value_df(data: pd.DataFrame, field: str) -> Any:
    """ Get the maximum value of a field in a DataFrame. """
    if data.empty:
        logger.warning("DataFrame is empty. Returning None.")
        return None
    if field not in data.columns:
        logger.error(f"Field '{field}' not found in DataFrame columns: {data.columns.tolist()}")
        raise KeyError(f"Field '{field}' not found in DataFrame columns.")
    return data[field].max()

def _get_max_field_value_json(data: list[dict], field: str) -> Any:
    """ Get the maximum value of a field in a list of dictionaries. """
    if not data:
        logger.warning("Data is empty. Returning None.")
        return None
    if not all(field in item for item in data):
        logger.error(f"Field '{field}' not found in all items.")
        raise KeyError(f"Field '{field}' not found in all items.")
    
    return max(item[field] for item in data if item[field] is not None)


def concat_io_content(data: IOContent, new_data: IOContent | dict | None) -> IOContent:
    """ Concatenate two data sources. """
    if new_data is None:
        logger.warning("New data is None. Returning original data.")
        return data
    elif data is None:
        return new_data
    elif isinstance(data, pd.DataFrame) and isinstance(new_data, pd.DataFrame):
        data = data.astype(str)
        new_data = new_data.astype(str)
        return pd.concat([data, new_data], ignore_index=True)
    elif isinstance(data, list) and isinstance(new_data, list):
        return data + new_data
    elif isinstance(data, list) and isinstance(new_data, dict):
        return data + [new_data]
    else:
        logger.debug(f"Data: {data}")
        logger.debug(f"New data: {new_data}")
        logger.error(f"Unsupported data types: {type(data)} and {type(new_data)}. Expected both to be of the same type.")
        raise TypeError(f"Unsupported data types: {type(data)} and {type(new_data)}. Expected both to be of the same type.")
