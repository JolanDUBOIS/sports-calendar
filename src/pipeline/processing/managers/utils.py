from __future__ import annotations
from typing import TYPE_CHECKING

import pandas as pd

from . import logger

if TYPE_CHECKING:
    from src.utils import IOContent


def inject_static_fields(data: IOContent, static_fields: list[dict] | None = None) -> IOContent:
    """ Inject static fields into the data. """
    if static_fields is None:
        return data
    
    if isinstance(data, list):
        return _inject_static_fields_json(data, static_fields)
    elif isinstance(data, pd.DataFrame):
        return _inject_static_fields_df(data, static_fields)
    else:
        logger.debug(f"Data type: {type(data)}")
        logger.debug(f"Static fields: {static_fields}")
        logger.error("Data should be either a list of dictionaries or a pandas DataFrame.")
        raise ValueError("Data should be either a list of dictionaries or a pandas DataFrame.")

def _inject_static_fields_json(data: list[dict], static_fields: list[dict]) -> list[dict]:
    """ Inject static fields into a list of dictionaries. """
    for item in data:
        for static_field in static_fields:
            key = static_field.get("name")
            value = static_field.get("value")
            if key not in item:
                item[key] = value
            else:
                logger.warning(f"Static field {key} already exists in the data.")
    return data

def _inject_static_fields_df(data: pd.DataFrame, static_fields: list[dict]) -> pd.DataFrame:
    """ Inject static fields into a DataFrame. """
    for static_field in static_fields:
        key = static_field.get("name")
        value = static_field.get("value")
        if key not in data.columns:
            data[key] = value
        else:
            logger.warning(f"Static field {key} already exists in the data.")
    return data