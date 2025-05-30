import json
from pathlib import Path

from . import logger, FileHandler


class JSONHandler(FileHandler):
    """ JSON file handler for reading and writing JSON files. """

    def _append(self, data: list[dict]) -> None:
        """ Append data to the JSON file. """
        self.content.extend(data)

    def _overwrite(self, data: list[dict]) -> None:
        """ Overwrite the JSON file with new data. """
        self.content = data

    @staticmethod
    def _read_file(file_path: Path) -> list[dict]:
        """ Read the JSON file and return its content. """
        try:
            with file_path.open("r") as f:
                data = json.load(f)
            return data
        except json.JSONDecodeError:
            logger.warning(f"JSON file {file_path} is empty.")
            return []
        except Exception as e:
            logger.error(f"Failed to read JSON file {file_path}: {e}")
            raise e

    @staticmethod
    def _write_file(file_path: Path, data: list[dict]) -> None:
        """ Write data to the JSON file. """
        if data is None:
            logger.warning(f"Data is None. File handler path: {file_path}")
            return
        try:
            with file_path.open("w") as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            logger.error(f"Failed to write JSON file {file_path}: {e}")
            raise e

    @staticmethod
    def _validate_data(data: list[dict], file_path: Path) -> None:
        """ TODO """
        if not isinstance(data, list):
            logger.error(f"Data must be a list. File handler path: {file_path}")
            raise ValueError(f"Data must be a list. File handler path: {file_path}")
        if not all(isinstance(d, dict) for d in data):
            logger.error(f"All elements in the data must be dictionaries. File handler path: {file_path}")
            raise ValueError(f"All elements in the data must be dictionaries. File handler path: {file_path}")
