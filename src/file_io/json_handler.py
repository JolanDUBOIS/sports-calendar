import json

from . import logger
from .base_file_handler import BaseFileHandler


class JSONHandler(BaseFileHandler):
    """ JSON file handler for reading and writing JSON files. """

    def _read_file(self) -> list[dict]:
        """ Read the JSON file and return its content. """
        if not self.path.exists():
            logger.debug(f"JSON file {self.path} does not exist. Returning empty list.")
            return []
        try:
            with self.path.open("r") as f:
                data = json.load(f)
            return data
        except json.JSONDecodeError:
            logger.warning(f"JSON file {self.path} is empty.")
            return []
        except Exception as e:
            logger.error(f"Failed to read JSON file {self.path}: {e}")
            raise e

    def _append(self, data: list[dict]) -> None:
        """ Append data to the JSON file. """
        logger.debug(f"Appending data to JSON file: {self.path}")
        content = self._read_file()
        content.extend(data)
        self._write_file(content)

    def _overwrite(self, data: list[dict]) -> None:
        """ Overwrite the JSON file with new data. """
        logger.debug(f"Overwriting JSON file with new data: {self.path}")
        self._write_file(data)

    def _write_file(self, data: list[dict]) -> None:
        """ Write data to the JSON file. """
        self._validate_data(data)
        try:
            with self.path.open("w") as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            logger.error(f"Failed to write JSON file {self.path}: {e}")
            raise e
    def _get_len(self) -> int:
        """ Return the length of the content. """
        content = self._read_file()
        self._validate_data(content)
        return len(content)

    def _validate_data(self, data: list[dict]) -> None:
        """ TODO """
        if not isinstance(data, list):
            logger.error(f"Data must be a list. File handler path: {self.path}")
            raise ValueError(f"Data must be a list. File handler path: {self.path}")
        if not all(isinstance(d, dict) for d in data):
            logger.error(f"All elements in the data must be dictionaries. File handler path: {self.path}")
            raise ValueError(f"All elements in the data must be dictionaries. File handler path: {self.path}")

    def _add_ctime(self, data: list[dict]) -> list[dict]:
        """ Add creation time to each dictionary in the list. """
        for item in data:
            if "_ctime" in item:
                logger.error(f"Data already contains '_ctime' key. File handler path: {self.path}")
                raise ValueError(f"Data already contains '_ctime' key. File handler path: {self.path}")
            item["_ctime"] = self._today()
        return data
