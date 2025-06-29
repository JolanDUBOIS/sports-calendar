from dataclasses import dataclass

import yaml
import pandas as pd

from . import logger


CONFIG_PATH = "config/date_standardization.yml"

def read_config(path: str) -> dict:
   """ Read YAML configuration file """
   with open(path, 'r') as file:
        return yaml.safe_load(file)

def date_standardization(data: pd.DataFrame, source_key: str, **kwargs) -> pd.DataFrame:
    """ Standardize date format """
    config = read_config(CONFIG_PATH)
    date_cols = config.get(source_key)
    if date_cols is None:
        logger.warning(f"Unknown source name: {source_key}")
        return data

    standardized_data = data.copy()
    for col, col_config in date_cols.items():
        if col not in data.columns:
            logger.warning(f"Column {col} not found in data for source {source_key}")
            continue
        date_format = col_config["format"]
        if date_format == "iso":
            tz_format = col_config.get("tz_format")
            if tz_format == {"Z", "naive"}:
                standardized_data[col] = pd.to_datetime(standardized_data[col], utc=True)
            elif tz_format == "+00:00":
                standardized_data[col] = pd.to_datetime(standardized_data[col])
            else:
                logger.error(f"Unsupported timezone format: {tz_format}")
                raise ValueError(f"Unsupported timezone format: {tz_format}")
        elif date_format == "timestamp":
            unit = col_config.get("unit", "s")
            standardized_data[col] = pd.to_datetime(standardized_data[col], unit=unit)
        else:
            logger.error(f"Unsupported date format: {date_format}")
            raise ValueError(f"Unsupported date format: {date_format}")

    return standardized_data
