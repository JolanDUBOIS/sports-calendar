from typing import Any
from pathlib import Path

import pandas as pd

from .file_handler import FileHandler
from src.data_processing.file_io import logger


class CSVHandler(FileHandler):
    """ CSV file handler. """

    def __init__(self, file_path: Path):
        super().__init__(file_path)

    def read(self, mode: str = "all", **kwargs) -> pd.DataFrame:
        """ TODO """
        logger.info(f"Reading data from {self.file_path}")
        df = pd.read_csv(self.file_path)
        if mode == "all":
            return df
        elif mode == "newest":
            version_field = kwargs.get("on", "created_at")
            version_type = kwargs.get("version_type", "datetime")
            version_threshold = kwargs.get("version_threshold")
            if version_threshold is None:
                version_threshold = 0
            logger.debug(f"Reading newest version with field: {version_field} and threshold: {version_threshold}")
            return self._get_newest_version(df, version_field, version_threshold, version_type)
        else:
            logger.error(f"Unsupported read mode: {mode}")
            raise ValueError(f"Unsupported read mode: {mode}")

    def write(self, data: pd.DataFrame, overwrite: bool = False) -> None:
        """ Write the data to a CSV file. """
        if data.empty:
            logger.warning("Data is empty. Nothing to write.")
            return
        logger.info(f"Writing data to {self.file_path}")
        if not self.file_path.exists() or (overwrite and self.confirm_overwrite()):
            data.to_csv(self.file_path, mode='w', header=True, index=False)
        else:
            try:
                existing_df = pd.read_csv(self.file_path)
            except pd.errors.EmptyDataError:
                existing_df = pd.DataFrame()
            data = pd.concat([existing_df, data], ignore_index=True)
            data.to_csv(self.file_path, mode='w', header=True, index=False)
        self._update_metadata(writer_type=self.__class__.__name__, rows=len(data))
        logger.debug(f"Data written to {self.file_path}")

    @staticmethod
    def _get_newest_version(
        data: pd.DataFrame,
        version_field: str,
        version_threshold: Any = None,
        version_type: str = 'datetime'
    ) -> pd.DataFrame:
        """ TODO """
        if version_field not in data.columns:
            logger.error(f"Version field '{version_field}' not found in DataFrame.")
            raise ValueError(f"Version field '{version_field}' not found in DataFrame.")

        if data[version_field].isnull().any():
            logger.error(f"Version field '{version_field}' contains null values.")
            raise ValueError(f"Version field '{version_field}' contains null values.")

        if version_type == 'datetime':
            data[version_field] = pd.to_datetime(data[version_field], errors='raise')
            version_threshold = pd.to_datetime(version_threshold, errors='raise') if version_threshold else pd.Timestamp('1900-01-01')
        elif version_type == 'numeric':
            data[version_field] = pd.to_numeric(data[version_field], errors='raise')
            version_threshold = float(version_threshold) if version_threshold else 0
        else:
            logger.error(f"Unsupported version type: {version_type}. Supported types are 'datetime' and 'numeric'.")
            raise ValueError(f"Unsupported version type: {version_type}. Supported types are 'datetime' and 'numeric'.")

        return data.loc[data[version_field] > version_threshold].copy()