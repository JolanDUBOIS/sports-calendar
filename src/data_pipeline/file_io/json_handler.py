import json
from pathlib import Path
from typing import Any

import pandas as pd

from .file_handler import FileHandler
from src.data_pipeline.file_io import logger


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
        data = [row for row in data if row.get(f'_version_{version_field}') is not None and row.get(f'_version_{version_field}') >= version_threshold]
        data = self._drop_field(data, f'_version_{version_field}')
        return data

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
            data = [row for row in data if row.get(f'_version_{version_field}') is not None and row.get(f'_version_{version_field}') <= version_threshold]
        else:
            data = [row for row in data if row.get(f'_version_{version_field}') is not None and row.get(f'_version_{version_field}') >= version_threshold]
        data = self._drop_field(data, f'_version_{version_field}')
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
                    row[f'_version_{version_field}'] = float(raw_value)
                else:  # datetime
                    row[f'_version_{version_field}'] = pd.to_datetime(raw_value, errors='raise')
            except (ValueError, TypeError):
                row[f'_version_{version_field}'] = None

        return data, version_threshold

    def _append(self, data: list[dict]) -> None:
        """ Append the data to the JSON file. """
        existing_data = self._read_json(self.file_path)
        if existing_data is None:
            existing_data = []
        self._validate_data(existing_data)
        existing_data.extend(data)
        self._write_json(self.file_path, existing_data)

    def _overwrite(self, data: list[dict]) -> None:
        """ Overwrite the JSON file with the data. """
        self._write_json(self.file_path, data)

    def _validate_data(self, data: list[dict]|None) -> None:
        """ Validate the structure of the JSON data. """
        if data is None:
            logger.warning(f"Data is None. File handler path: {self.file_path}")
        elif not isinstance(data, (list)):
            logger.error(f"Data must be a list. File handler path: {self.file_path}")
            raise ValueError(f"Data must be a list. File handler path: {self.file_path}")
        elif not all(isinstance(d, dict) for d in data):
            logger.error(f"All elements in the data must be dictionaries. File handler path: {self.file_path}")
            raise ValueError(f"All elements in the data must be dictionaries. File handler path: {self.file_path}")

    @staticmethod
    def _read_json(file_path: Path) -> dict|list|None:
        """ Read a JSON file and return its content. """
        try:
            with file_path.open("r") as f:
                data = json.load(f)
            return data
        except json.JSONDecodeError:
            logger.warning(f"JSON file {file_path} is empty.")
            return None
        except Exception as e:
            logger.error(f"Failed to read JSON file {file_path}: {e}")
            raise e

    @staticmethod
    def _write_json(file_path: Path, data: dict|list|None) -> None:
        """ Write data to a JSON file. """
        if data is None:
            logger.warning(f"Data is None. File handler path: {file_path}")
            return
        with file_path.open("w") as f:
            json.dump(data, f, indent=4)

    @staticmethod
    def _drop_field(data: list[dict], field: str) -> list[dict]:
        """ Drop a field from the data. """
        for row in data:
            row.pop(field, None)
        return data
