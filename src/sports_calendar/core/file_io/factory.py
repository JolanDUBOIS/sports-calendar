from pathlib import Path

from . import logger
from .base_file_handler import BaseFileHandler
from .csv_handler import CSVHandler
from .json_handler import JSONHandler


class FileHandlerFactory:
    """ Factory class to create file handlers. """

    @staticmethod
    def create_file_handler(file_path: Path | str) -> BaseFileHandler:
        """ Create a file handler based on the file path. """
        logger.debug(f"Creating file handler for path: {file_path}")
        file_path = Path(file_path).resolve()
        file_suffix = file_path.suffix.lower()
        if file_suffix == ".csv":
            return CSVHandler(file_path)
        elif file_suffix == ".json":
            return JSONHandler(file_path)
        else:
            logger.error(f"Unsupported file type: {file_suffix}. Supported types are: .csv, .json")
            raise ValueError(f"Unsupported file type: {file_suffix}. Supported types are: .csv, .json")
