import json
from typing import Any
from datetime import datetime
from pathlib import Path
from abc import ABC, abstractmethod

import pandas as pd

from src.data_processing import logger


class FileHandler(ABC):
    """ Abstract base class for file handlers. """

    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.now = datetime.now().isoformat(timespec="seconds")
        self.meta_path = self.file_path.parent / ".meta.json"

    @property
    def path(self) -> Path:
        """ Return the file path. """
        return self.file_path

    @abstractmethod
    def read(self, mode: str = "all", **kwargs) -> Any:
        """ Read the file and return its content. """
        pass

    @abstractmethod
    def write(self, data: Any, overwrite: bool = False, **kwargs) -> None:
        """ Write the data to the file. """
        pass

    def confirm_overwrite(self) -> bool:
        """ Ask the user to confirm overwriting the file. """
        response = input(f"Are you sure you want to overwrite the file {self.file_path}? (yes/y to confirm): ").strip().lower()
        return response in ['yes', 'y']

    def _update_metadata(self, writer_type: str, rows: int) -> None:
        """ TODO """
        meta = {}
        if self.meta_path.exists():
            try:
                with self.meta_path.open("r") as f:
                    meta = json.load(f)
            except Exception as e:
                logger.error(f"Failed to read existing metadata: {e}")
                raise e

        meta[self.file_path.name] = {
            "last_written": self.now,
            "writer": writer_type,
            "rows": rows
        }

        with self.meta_path.open("w") as f:
            json.dump(meta, f, indent=4)
        logger.debug(f"Metadata updated at {self.meta_path}")

    def last_update(self) -> str:
        """ Get the last update time of the file. """
        logger.debug(f"Getting last update time for {self.file_path}")
        if self.meta_path.exists():
            try:
                with self.meta_path.open("r") as f:
                    meta = json.load(f)
                logger.debug(f"Metadata read: {meta}")
                return meta.get(self.file_path.name, {}).get("last_written", None)
            except Exception as e:
                logger.error(f"Failed to read metadata: {e}")
                raise e
        else:
            logger.warning(f"Metadata file {self.meta_path} does not exist.")
            return None

    @staticmethod
    @abstractmethod
    def _get_newest_version(data: list[dict]|pd.DataFrame, version_field: str, version_threshold: Any = None, version_type: str = 'datetime') -> list[dict]|pd.DataFrame:
        """ TODO """
        pass

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
    def _get_newest_version(data: pd.DataFrame, version_field: str, version_threshold: Any = None, version_type: str = 'datetime') -> pd.DataFrame:
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

class JSONHandler(FileHandler):
    """ JSON file writer. """
    
    def read(self, mode: str = "all", **kwargs) -> dict:
        """ TODO """
        logger.info(f"Reading data from {self.file_path}")
        with self.file_path.open("r") as f:
            data = json.load(f)
        if not isinstance(data, list):
            logger.error(f"Data in the file {self.file_path} is not a list.")
            raise ValueError(f"Data in the file {self.file_path} is not a list.")
        if mode == "all":
            return data
        elif mode == "newest":
            version_field = kwargs.get("on", "created_at")
            version_type = kwargs.get("version_type", "datetime")
            version_threshold = kwargs.get("version_threshold")
            if version_threshold is None:
                version_threshold = 0
            logger.debug(f"Reading newest version with field: {version_field} and threshold: {version_threshold}")
            return self._get_newest_version(data, version_field, version_threshold, version_type)
        else:
            logger.error(f"Unsupported read mode: {mode}")
            raise ValueError(f"Unsupported read mode: {mode}")            

    def write(self, data: list[dict], overwrite: bool = False) -> None:
        """ Write the data to a JSON file. """
        if not data:
            logger.warning("Data is empty. Nothing to write.")
            return
        logger.info(f"Writing data to {self.file_path}")
        if not isinstance(data, list):
            logger.error("Data must be a list.")
            raise ValueError("Data must be a list.")

        if not self.file_path.exists() or (overwrite and self.confirm_overwrite()):
            with self.file_path.open(mode='w') as file:
                json.dump(data, file, indent=4)
        else:
            # TODO - Maybe use .jsonl ?
            with self.file_path.open(mode='r') as file:
                existing_data = json.load(file)
            if not isinstance(existing_data, list):
                logger.error("Existing data in the file is not a list.")
                raise ValueError("Existing data in the file is not a list.")
            existing_data.extend(data)
            with self.file_path.open(mode='w') as file:
                json.dump(existing_data, file, indent=4)
        self._update_metadata(writer_type=self.__class__.__name__, rows=len(data))
        logger.debug(f"Data written to {self.file_path}")

    @staticmethod
    def _get_newest_version(data: list[dict], version_field: str, version_threshold: Any = None, version_type: str = 'datetime') -> list:
        """ TODO """
        if not all(isinstance(d, dict) for d in data):
            logger.error("All elements in the list must be dictionaries.")
            raise ValueError("All elements in the list must be dictionaries.")

        version_list = [d.get(version_field) for d in data]
        if any(v is None for v in version_list):
            logger.error(f"Version field '{version_field}' contains null values.")
            raise ValueError(f"Version field '{version_field}' contains null values.")

        if version_type == 'datetime':
            version_list = [pd.to_datetime(v, errors='raise') for v in version_list]
            version_threshold = pd.to_datetime(version_threshold, errors='raise') if version_threshold else pd.Timestamp('1900-01-01')
        elif version_type == 'numeric':
            version_list = [float(v) for v in version_list]
            version_threshold = float(version_threshold) if version_threshold else 0
        else:
            logger.error(f"Unsupported version type: {version_type}. Supported types are 'datetime' and 'numeric'.")
            raise ValueError(f"Unsupported version type: {version_type}. Supported types are 'datetime' and 'numeric'.")

        mask = [v > version_threshold for v in version_list]
        return [d for d, m in zip(data, mask) if m]

class FileHandlerFactory:
    """ Factory class to create file handlers. """

    @staticmethod
    def create_file_handler(file_path: Path) -> FileHandler:
        """ Create a file handler based on the file extension. """
        if file_path.suffix == ".csv":
            return CSVHandler(file_path)
        elif file_path.suffix == ".json":
            return JSONHandler(file_path)
        else:
            logger.error(f"Unsupported file type: {file_path.suffix}")
            raise ValueError(f"Unsupported file type: {file_path.suffix}")
