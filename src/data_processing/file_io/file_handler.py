import json
from typing import Tuple, TypeAlias, Any
from datetime import datetime
from pathlib import Path
from abc import ABC, abstractmethod

import pandas as pd

from src.data_processing.file_io import logger


FileContent: TypeAlias = list[dict] | pd.DataFrame | None

class FileHandler(ABC):
    """ Abstract base class for file handlers. """

    def __init__(self, file_path: Path):
        """ Initialize the file handler """
        self._check_path(file_path)
        self.file_path = file_path
        self.now = datetime.now().isoformat(timespec="seconds")
        self.meta_path = self.file_path.parent / ".meta.json"

    @staticmethod
    def _check_path(file_path: Path) -> None:
        """ Check if the file path is valid. """
        if file_path.exists() and not file_path.is_file():
            logger.error(f"{file_path} is not a file.")
            raise ValueError(f"{file_path} is not a file.")

    @property
    def path(self) -> Path:
        """ Return the file path. """
        return self.file_path

    # Read methods

    def read(self, mode: str = "all", **kwargs) -> FileContent:
        """ Read the file and return its content. """
        logger.info(f"Reading data from {self.file_path}")
        if mode == "all":
            data = self._read_all()
        elif mode == "newest":
            version_field = kwargs.get("on", "created_at")
            version_threshold = kwargs.get("version_threshold") if "version_threshold" in kwargs else 0
            logger.debug(f"Reading newest version with field: {version_field} and threshold: {version_threshold}")
            data = self._read_newest(version_field, version_threshold)
        else:
            logger.error(f"Unsupported read mode: {mode}")
            raise ValueError(f"Unsupported read mode: {mode}")
        self._validate_data(data)
        return data

    @abstractmethod
    def _read_all(self) -> FileContent:
        """ Read all data from the file. """
        pass

    @abstractmethod
    def _read_newest(self, version_field: str, version_threshold: Any = None) -> FileContent:
        """ Read the newest version of the data. """
        pass

    # Delete methods

    @abstractmethod
    def delete_records(self, version_field: str, version_threshold: Any = None, delete_newest: bool = False) -> None:
        """ Delete records from the file based on the version field and threshold. """
        pass

    # Write methods

    def write(self, data: FileContent, overwrite: bool = False, **kwargs) -> None:
        """ Write the data to the file. """
        self._validate_data(data)
        if data is None or len(data) == 0:
            logger.warning("Data is empty. Nothing to write.")
            return
        logger.info(f"Writing data to {self.file_path}")
        if not self.file_path.exists() or self.file_path.stat().st_size == 0 or (overwrite and self.confirm_overwrite()):
            logger.debug(f"Overwriting file {self.file_path}")
            self._overwrite(data)
        else:
            logger.debug(f"Appending to file {self.file_path}")
            self._append(data)
        self._update_metadata(self.__class__.__name__, len(data))
        logger.debug(f"Data written to {self.file_path}")

    @abstractmethod
    def _append(self, data: FileContent) -> None:
        """ Write the data to the file. """
        pass
    
    @abstractmethod
    def _overwrite(self, data: FileContent) -> None:
        """ Overwrite the file with the new data. """
        pass

    def confirm_overwrite(self) -> bool:
        """ Ask the user to confirm overwriting the file. """
        response = input(f"Are you sure you want to overwrite the file {self.file_path}? (yes/y to confirm): ").strip().lower()
        return response in ['yes', 'y']

    # Metadata methods

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

    # Helper methods

    @abstractmethod
    def _prepare_version_field(
        self,
        data: FileContent,
        version_field: str,
        version_threshold: Any
    ) -> tuple[FileContent, float|pd.Timestamp|None]:
        """ Prepare the version field for the data. """
        pass

    @abstractmethod
    def _validate_data(self, data: FileContent) -> None:
        """ Validate the data after reading or before writing. """
        pass

    @staticmethod
    def _parse_version_value(value: Any) -> Tuple[float|pd.Timestamp, str]:
        """ Parse the version value to determine its type. """
        try:
            return float(value), 'numeric'
        except (ValueError, TypeError):
            try:
                return pd.to_datetime(value, errors='raise'), 'datetime'
            except (ValueError, TypeError):
                logger.error(f"The provided value {value} is not a valid datetime or numeric value.")
                raise ValueError(f"The provided value {value} is not a valid datetime or numeric value.")
