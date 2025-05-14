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
        data, version_threshold = self._prepare_version_field(data, version_field, version_threshold)
        if version_threshold is None:
            return data.copy()
        if len(data) == 0:
            logger.warning("No data found in the version field.")
            return data.copy()
        return [row for row in data if row.get(version_field) is not None and row.get(version_field) > version_threshold]

    def delete_records(self, version_field: str, version_threshold: Any = None, delete_newest: bool = False) -> None:
        """ Delete records from the JSON file based on the version field and threshold. """
        data = self._read_json(self.file_path)
        data, version_threshold = self._prepare_version_field(data, version_field, version_threshold)
        if version_threshold is None:
            return
        if len(data) == 0:
            logger.warning("No data found in the version field.")
            return
        if delete_newest:
            data = [row for row in data if row.get(version_field) is not None and row.get(version_field) < version_threshold]
        else:
            data = [row for row in data if row.get(version_field) is not None and row.get(version_field) > version_threshold]
        self.write(data, overwrite=True)

    def _prepare_version_field(
        self,
        data: list[dict],
        version_field: str,
        version_threshold: Any
    ) -> tuple[list[dict], float|pd.Timestamp|None]:
        """ Clean and convert the version_field, return cleaned data and parsed threshold. """
        if version_threshold is None:
            return data, None
        self._validate_data(data)
        version_threshold, version_type = self._parse_version_value(version_threshold)

        for row in data:
            raw_value = row.get(version_field)
            if raw_value is None:
                continue
            try:
                if version_type == 'numeric':
                    row[version_field] = float(raw_value)
                else:  # datetime
                    row[version_field] = pd.to_datetime(raw_value, errors='raise')
            except (ValueError, TypeError):
                row[version_field] = None

        return data, version_threshold

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
