from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime, timezone

from . import logger
from .metadata_manager import MetadataManager
from sports_calendar.sc_core import IOContent


class BaseFileHandler(ABC):
    """ Abstract base class for file handlers. """

    def __init__(self, file_path: str | Path):
        """ Initialize the file handler. """
        self.file_path = Path(file_path).resolve()
        self._check_path(self.file_path)
        self.meta_manager = MetadataManager(self.file_path)

    def __len__(self) -> int:
        """ Return the length of the content. """
        return self._get_len()

    def __repr__(self):
        """ Return a string representation of the file handler. """
        return f"{self.__class__.__name__}(path={self.path})"

    @property
    def path(self) -> Path:
        """ Return the file path. """
        return self.file_path

    @staticmethod
    def _check_path(file_path: Path) -> None:
        """ Check if the file path is valid. """
        if not file_path.parent.exists():
            logger.warning(f"The directory {file_path.parent} does not exist. Creating it.")
            file_path.parent.mkdir(parents=True, exist_ok=True)
        if file_path.exists() and not file_path.is_file():
            logger.error(f"The path {file_path} is not a valid file.")
            raise FileNotFoundError(f"The path {file_path} is not a valid file.")
        if not file_path.exists():
            logger.debug(f"The file {file_path} does not exist. It will be created on write.")

    def read(self) -> IOContent:
        """ Read the content of the file. """
        return self._read_file()

    def write(self, data: IOContent, source_versions: dict | None = None, overwrite: bool = False) -> None:
        """ TODO """
        data = self._add_ctime(data)
        try:
            added = len(data)
            if overwrite:
                removed = self.__len__()
                self._overwrite(data)
            else:
                removed = 0
                self._append(data)
        except Exception as e:
            logger.error(f"Failed to write data to {self.path}: {e}")
            raise
        self.meta_manager.record_write(
            added=added,
            removed=removed,
            source_versions=source_versions or {}
        )
        logger.info(f"Data written to {self.path} successfully.")

    def delete(self, force: bool = False) -> None:
        """ Delete data from the file. """
        removed = self.__len__()
        if self.path.exists():
            if force or self._confirm_delete():
                self.path.unlink()
                logger.info(f"File {self.path} deleted successfully.")
            else:
                logger.info(f"File deletion for {self.path} was cancelled.")
        else:
            logger.warning(f"File {self.path} does not exist, nothing to delete.")
        self.meta_manager.record_delete(removed=removed)

    def _confirm_delete(self) -> bool:
        """ Ask the user to confirm deleting the file. """
        response = input(f"Are you sure you want to delete the file {self.path}? (yes/y to confirm): ").strip().lower()
        return response in ['yes', 'y']

    @abstractmethod
    def cleanup(self, cutoff: str) -> None:
        """ Clean up old data based on the cutoff date (ISO format). """

    @abstractmethod
    def _read_file(self) -> IOContent:
        """ TODO """

    @abstractmethod
    def _append(self, data: IOContent) -> None:
        """ Append data to the file using _write_file() method. """
    
    @abstractmethod
    def _overwrite(self, data: IOContent) -> None:
        """ Overwrite the file with the given data using _write_file() method. """

    @abstractmethod
    def _write_file(self, data: IOContent) -> None:
        """ TODO """

    @abstractmethod
    def _get_len(self) -> int:
        """ Return the length of the content. """

    @abstractmethod
    def _validate_data(self, data: IOContent) -> None:
        """ TODO """

    @abstractmethod
    def _add_ctime(data: IOContent) -> IOContent:
        """ Add creation time to the data. """

    @staticmethod
    def _today() -> str:
        """ Return today's date in ISO format. """
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    @staticmethod
    def _check_iso_format(date_str: str) -> None:
        """ Check if the date string is in ISO format. """
        try:
            datetime.fromisoformat(date_str)
        except ValueError:
            logger.error(f"Date string '{date_str}' is not in ISO format.")
            raise ValueError(f"Date string '{date_str}' is not in ISO format.")
