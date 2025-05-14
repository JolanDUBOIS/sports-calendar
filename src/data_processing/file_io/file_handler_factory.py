from pathlib import Path

from .file_handler import FileHandler
from .csv_handler import CSVHandler
from .json_handler import JSONHandler
from src.data_processing.file_io import logger


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