from typing import Any

import pandas as pd

from . import logger
from sports_calendar.core import IOContent


def filter_file_content(data: IOContent, field: str, op: str, value: Any | None = None) -> IOContent:
    """ Filter the data based on a field, operator, and value. """
    logger.debug(f"Filtering data with field='{field}', operator='{op}', value='{value}'.")
    if value is None:
        logger.debug("No value provided for filtering. Returning original data.")
        return data
    if isinstance(data, pd.DataFrame):
        return _filter_df(data, field, op, value)
    elif isinstance(data, list):
        return _filter_json(data, field, op, value)
    else:
        logger.error(f"Unsupported data type: {type(data)}. Expected pd.DataFrame or list.")
        raise TypeError(f"Unsupported data type: {type(data)}. Expected pd.DataFrame or list.")

def _filter_df(data: pd.DataFrame, field: str, op: str, value: Any) -> pd.DataFrame:
    """ Filter a DataFrame based on a field, operator, and value. """
    if data.empty:
        logger.warning("DataFrame is empty. Returning an empty DataFrame.")
        return pd.DataFrame()

    if field not in data.columns:
        logger.error(f"Field '{field}' not found in DataFrame columns: {data.columns.tolist()}")
        raise KeyError(f"Field '{field}' not found in DataFrame columns.")

    if _is_datetime_string(value):
        data[f"__temp_filter__{field}"] = pd.to_datetime(data[field], errors='coerce')
        value = pd.to_datetime(value, errors='coerce')
    else:
        data[f"__temp_filter__{field}"] = data[field]

    ops = {
        '==': data[f"__temp_filter__{field}"] == value,
        '!=': data[f"__temp_filter__{field}"] != value,
        '<': data[f"__temp_filter__{field}"] < value,
        '<=': data[f"__temp_filter__{field}"] <= value,
        '>': data[f"__temp_filter__{field}"] > value,
        '>=': data[f"__temp_filter__{field}"] >= value,
    }

    try:
        return data[ops[op]].drop(columns=[f"__temp_filter__{field}"])
    except KeyError:
        logger.error(f"Unsupported operator '{op}'. Supported operators are: {list(ops.keys())}")
        raise ValueError(f"Unsupported operator '{op}'. Supported operators are: {list(ops.keys())}")

def _filter_json(data: list[dict], field: str, op: str, value: Any) -> list[dict]:
    """ Filter a list of dictionaries based on a field, operator, and value. """
    if not data:
        logger.warning("Data is empty. Returning an empty list.")
        return []

    if _is_datetime_string(value):
        value_type  = 'datetime'
        value = pd.to_datetime(value, errors='coerce')
    else:
        value_type  = 'other'
    
    def get_val(item: dict) -> Any:
        raw = item.get(field)
        if raw is None:
            return None
        if value_type  == 'datetime':
            return pd.to_datetime(raw, errors='coerce')
        else:
            return raw

    ops = {
        '==': lambda a: a == value,
        '!=': lambda a: a != value,
        '<': lambda a: a < value,
        '<=': lambda a: a <= value,
        '>': lambda a: a > value,
        '>=': lambda a: a >= value,
    }
    
    try:
        return [item for item in data if ops[op](get_val(item))]
    except KeyError:
        logger.error(f"Unsupported operator '{op}'. Supported operators are: {list(ops.keys())}")
        raise ValueError(f"Unsupported operator '{op}'. Supported operators are: {list(ops.keys())}")

def _is_datetime_string(val: Any) -> bool:
    """ Heuristic to guess if a string is a datetime. """
    if not isinstance(val, str):
        return False
    try:
        pd.to_datetime(val)
        return True
    except Exception:
        return False
