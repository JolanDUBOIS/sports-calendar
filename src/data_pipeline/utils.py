from pathlib import Path
from typing import Any
from datetime import datetime, timedelta

import yaml
import pandas as pd

from . import logger
from .types import IOContent


def date_offset_constructor(loader, node):
    """ Custom YAML constructor to handle date offsets. """
    days = int(node.value)
    return (datetime.today() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

def include_constructor(loader, node):
    """ Custom YAML constructor to include part of another YAML file. """
    value = loader.construct_mapping(node)
    file = value["file"]
    model_name = value["model_name"]

    # Resolve path relative to the current YAML file being loaded
    base_path = Path(loader.name).parent
    include_path = base_path / file

    # Load the included YAML content
    with open(include_path, "r") as f:
        included_yaml = yaml.safe_load(f)

    # Extract the corresponding model's column mapping
    for model in included_yaml.get("models", []):
        if model.get("name") == model_name:
            return model.get("columns_mapping", {})

    raise ValueError(f"No model named '{model_name}' found in '{file}'")

yaml.add_constructor('!date_offset', date_offset_constructor, Loader=yaml.loader.SafeLoader)
yaml.add_constructor('!include', include_constructor, Loader=yaml.SafeLoader)


def read_yml_file(file_path: Path) -> dict:
    """ Read a YAML file and return its content as a dictionary. """
    with file_path.open(mode='r') as file:
        config = yaml.safe_load(file)
    return config


def filter_file_content(data: IOContent, field: str, op: str, value: Any, type: str | None = None) -> IOContent:
    """ Filter the data based on a field, operator, and value. """
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


def get_max_field_value(data: IOContent, field: str) -> Any:
    """ Get the maximum value of a field in the data. """
    if isinstance(data, pd.DataFrame):
        return _get_max_field_value_df(data, field)
    elif isinstance(data, list):
        return max((item.get(field) for item in data if field in item), default=None)
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
