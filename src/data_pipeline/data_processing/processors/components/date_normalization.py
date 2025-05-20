from zoneinfo import ZoneInfo

import pandas as pd

from . import logger


MAPPING_SOURCE_TO_DATE_PARAMS = {
    "live_soccer_matches": {"date_column": "source_time", "unit": "ms"},
    "football_data_matches": {"date_column": "utcDate"},
    "espn_matches": {"date_column": "date"},
}

def date_normalization(data: pd.DataFrame, source_key: str, **kwargs) -> pd.DataFrame:
    """ Normalize date format """
    date_params = MAPPING_SOURCE_TO_DATE_PARAMS.get(source_key)
    if not date_params:
        logger.debug(f"Unknown source name: {source_key}")
        return data

    return _normalize_date(data, **date_params)

def _normalize_date(data: pd.DataFrame, date_column: str, unit: str = None) -> pd.DataFrame:
    """ Normalize date format """
    data["date_time_utc"] = pd.to_datetime(data[date_column], unit=unit, utc=True)
    data["date_time_cet"] = data["date_time_utc"].dt.tz_convert(ZoneInfo("Europe/Paris"))
    return data
