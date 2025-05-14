import json
from typing import Any
from datetime import datetime
from pathlib import Path
from abc import ABC, abstractmethod

import pandas as pd

from src.data_processing.file_io import logger


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
    def _get_newest_version(
        data: list[dict]|pd.DataFrame,
        version_field: str,
        version_threshold: Any = None,
        version_type: str = 'datetime'
    ) -> list[dict]|pd.DataFrame:
        """ TODO """
        pass
