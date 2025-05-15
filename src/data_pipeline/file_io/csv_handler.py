from typing import Any

import pandas as pd

from .file_handler import FileHandler
from src.data_pipeline.file_io import logger


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
        data = data.dropna(subset=[f'_version_{version_field}'])
        data = data[data[f'_version_{version_field}'] >= version_threshold].copy()
        data = data.drop(columns=[f'_version_{version_field}'])
        return data

    def delete_records(self, version_field: str, version_threshold: Any = None, delete_newest: bool = False) -> None:
        """ Delete records from the CSV file based on the version field and threshold. """
        data = pd.read_csv(self.file_path)
        data, version_threshold = self._prepare_version_field(data, version_field, version_threshold)
        if version_threshold is None:
            return
        data = data.dropna(subset=[f'_version_{version_field}'])
        if delete_newest:
            data = data[data[f'_version_{version_field}'] <= version_threshold]
        else:
            data = data[data[f'_version_{version_field}'] >= version_threshold]
        data = data.drop(columns=[f'_version_{version_field}'])
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
            data[f'_version_{version_field}'] = pd.to_numeric(data[version_field], errors='coerce')
        elif version_type == 'datetime':
            data[f'_version_{version_field}'] = pd.to_datetime(data[version_field], errors='coerce')

        return data, version_threshold

    def _append(self, data: pd.DataFrame) -> None:
        """ Append the data to the CSV file. """
        data.to_csv(self.file_path, mode='a', header=False, index=False)

    def _overwrite(self, data: pd.DataFrame) -> None:
        """ Overwrite the CSV file with the data. """
        data.to_csv(self.file_path, mode='w', header=True, index=False)

    def _validate_data(self, data: pd.DataFrame) -> None:
        """ Validate the data. """
        if data is None:
            logger.warning(f"Data is None. File handler path: {self.file_path}")
        if not isinstance(data, pd.DataFrame):
            logger.error(f"Data is not a pandas DataFrame. File handler path: {self.file_path}")
            raise ValueError(f"Data must be a pandas DataFrame. File handler path: {self.file_path}")
