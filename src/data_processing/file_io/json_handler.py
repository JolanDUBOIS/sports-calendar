import json
from pathlib import Path
from typing import Any

import pandas as pd

from .file_handler import FileHandler
from src.data_processing.file_io import logger


class JSONHandler(FileHandler):
    """ JSON file writer. """

    def _read_all(self) -> list[dict]:
        """ Read all data from the JSON file. """
        return self._read_json(self.file_path)
    
    def _read_newest(self, version_field: str, version_threshold: Any = None) -> list[dict]:
        """ Read the newest version of the data. """
        data = self._read_json(self.file_path)
        if version_threshold is None:
            return data
        self._validate_data(data)

        version_threshold, version_type = self._parse_version_value(version_threshold)

        filtered_data = []
        for row in data:
            raw_value = row.get(version_field)
            if raw_value is None:
                continue

            try:
                if version_type == 'numeric':
                    value = float(raw_value)
                else:  # datetime
                    value = pd.to_datetime(raw_value, errors='raise')
            except (ValueError, TypeError):
                continue
            
            if value >= version_threshold:
                filtered_data.append(row)
        
        return filtered_data

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

    def _validate_data(self, data: list[dict]|None) -> None:
        """ Validate the structure of the JSON data. """
        if data is None:
            logger.warning(f"Data at path {self.path} is None.")
        elif not isinstance(data, (list)):
            logger.error("Data must be a list.")
            raise ValueError("Data must be a list.")
        elif not all(isinstance(d, dict) for d in data):
            logger.error("All elements in the data must be dictionaries.")
            raise ValueError("All elements in the data must be dictionaries.")

    @staticmethod
    def _read_json(file_path: Path) -> dict|list|None:
        """ Read a JSON file and return its content. """
        try:
            with file_path.open("r") as f:
                data = json.load(f)
            return data
        except Exception as e:
            logger.error(f"Failed to read JSON file {file_path}: {e}")
            raise e
