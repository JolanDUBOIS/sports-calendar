from typing import Any

import pandas as pd

from .file_handler import FileHandler
from src.data_processing.file_io import logger


class CSVHandler(FileHandler):
    """ CSV file handler. """

    def _read_all(self) -> pd.DataFrame:
        """ Read all data from the CSV file. """
        return pd.read_csv(self.file_path)

    def _read_newest(self, version_field: str, version_threshold: Any = None) -> pd.DataFrame:
        """ Read the newest version of the data. """
        data = pd.read_csv(self.file_path)
        data, version_threshold = self._prepare_version_field(data, version_field, version_threshold)
        if version_threshold is None:
            return data.copy()
        data = data.dropna(subset=[version_field])
        return data[data[version_field] > version_threshold].copy()

    def delete_records(self, version_field: str, version_threshold: Any = None, delete_newest: bool = False) -> None:
        """ Delete records from the CSV file based on the version field and threshold. """
        data = pd.read_csv(self.file_path)
        data, version_threshold = self._prepare_version_field(data, version_field, version_threshold)
        if version_threshold is None:
            return

        data = data.dropna(subset=[version_field])
        if delete_newest:
            data = data[data[version_field] < version_threshold]
        else:
            data = data[data[version_field] > version_threshold]
            
        self.write(data, overwrite=True)

    def _prepare_version_field(
        self,
        data: pd.DataFrame,
        version_field: str,
        version_threshold: Any
    ) -> tuple[pd.DataFrame, float|pd.Timestamp|None]:
        """ Clean and convert the version_field, return cleaned data and parsed threshold. """
        if version_threshold is None:
            return data, None

        if version_field not in data.columns:
            logger.warning(f"Version field '{version_field}' not found in DataFrame.")
            return data, None

        version_threshold, version_type = self._parse_version_value(version_threshold)

        if version_type == 'numeric':
            data[version_field] = pd.to_numeric(data[version_field], errors='coerce')
        elif version_type == 'datetime':
            data[version_field] = pd.to_datetime(data[version_field], errors='coerce')

        return data, version_threshold

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
