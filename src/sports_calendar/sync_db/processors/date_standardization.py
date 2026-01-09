import numpy as np
import pandas as pd

from . import logger
from .base import Processor
from sports_calendar.sync_db.definitions.specs import ProcessingIOInfo


class DateStandardizationProcessor(Processor):
    """ Processor to standardize date formats in a DataFrame. """
    config_filename = "date_standardization"

    @classmethod
    def _run(cls, data: dict[str, pd.DataFrame], io_info: ProcessingIOInfo, **kwargs) -> pd.DataFrame:
        """ Standardize date formats in the specified DataFrame. """
        df = data.get("data").copy()
        if df is None:
            logger.error("Input data not found in the provided data dictionary.")
            raise ValueError("Input data not found in the provided data dictionary.")
        if df.empty:
            logger.warning("Input DataFrame is empty. No standardization will be performed.")
            return df

        config = cls.load_config(io_info.config_key)

        standardized_data = df.copy()
        for col, col_config in config.items():
            logger.debug(f"Standardizing column: {col} with config: {col_config}")
            if col not in df.columns:
                logger.warning(f"Column {col} not found in data. Skipping standardization for this column.")
                continue
            date_format = col_config["format"]
            if date_format == "iso":
                tz_format = col_config.get("tz_format")
                if tz_format in {"Z", "naive"}:
                    dt_series = pd.to_datetime(standardized_data[col], utc=True)
                elif tz_format == "+00:00":
                    dt_series = pd.to_datetime(standardized_data[col])
                else:
                    logger.error(f"Unsupported timezone format: {tz_format}")
                    raise ValueError(f"Unsupported timezone format: {tz_format}")
            elif date_format == "timestamp":
                unit = col_config.get("unit", "s")
                dt_series = pd.to_datetime(standardized_data[col], unit=unit)
            else:
                logger.error(f"Unsupported date format: {date_format}")
                raise ValueError(f"Unsupported date format: {date_format}")

        standardized_data[col] = dt_series.apply(lambda x: x.isoformat() if pd.notnull(x) else None)

        return standardized_data
