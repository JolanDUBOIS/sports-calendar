from zoneinfo import ZoneInfo

import pandas as pd

from .processor_base_class import Processor
from src.data_processing import logger


class DateNormalization(Processor):
    """ Normalize date format for different data sources """

    mapping_source_to_date_params = {
        "live_soccer_matches": {"date_column": "source_time", "unit": "ms"},
        "football_data_matches": {"date_column": "utcDate"},
        "espn_matches": {"date_column": "date"},
    }

    def process(self, data: pd.DataFrame, source_key: str, **kwargs) -> pd.DataFrame:
        """ Process the data """
        date_params = self.mapping_source_to_date_params.get(source_key)
        if not date_params:
            logger.debug(f"Unknown source name: {source_key}")
            return data

        return self.normalize_date(data, **date_params)

    @staticmethod
    def normalize_date(data: pd.DataFrame, date_column: str, unit: str = None) -> pd.DataFrame:
        """ Normalize date format """
        data["date_time_utc"] = pd.to_datetime(data[date_column], unit=unit, utc=True)
        data["date_time_cet"] = data["date_time_utc"].dt.tz_convert(ZoneInfo("Europe/Paris"))
        return data
