import json
from typing import Any

import pandas as pd

from .file_handler import FileHandler
from src.data_processing.file_io import logger


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
    def _get_newest_version(
        data: list[dict],
        version_field: str,
        version_threshold: Any = None,
        version_type: str = 'datetime'
    ) -> list:
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