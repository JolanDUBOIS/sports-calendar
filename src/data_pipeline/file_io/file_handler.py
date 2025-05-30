from pathlib import Path
from typing import TypeAlias, Any
from abc import ABC, abstractmethod

import pandas as pd

from . import logger
from ..types import IOContent


class FileHandler(ABC):
    """ Abstract base class for file handlers. """

    def __init__(self, file_path: str | Path):
        """ Initialize the file handler """
        self.file_path = Path(file_path).resolve()
        self._check_path(self.file_path)
        self.content: IOContent = self._read_file(self.file_path)

    @property
    def path(self) -> Path:
        """ Return the file path. """
        return self.file_path

    @staticmethod
    def _check_path(file_path: Path) -> None:
        """ Check if the file path is valid and exists. """
        if not file_path.parent.exists():
            logger.error(f"The directory {file_path.parent} does not exist.")
            raise FileNotFoundError(f"The directory {file_path.parent} does not exist.")
        if file_path.exists() and not file_path.is_file():
            logger.error(f"The path {file_path} is not a valid file.")
            raise FileNotFoundError(f"The path {file_path} is not a valid file.")
        file_path.touch(exist_ok=True)

    def __len__(self) -> int:
        """ Return the length of the content. """
        try:
            return len(self.content)
        except TypeError:
            logger.warning(f"Content of {self.path} is not iterable, returning length as 0.")
            return 0

    def __repr__(self):
        """ Return a string representation of the file handler. """
        return f"{self.__class__.__name__}(path={self.path})"

    def read(self) -> IOContent:
        """ Return the content of the file. """
        return self.content

    def write(self, data: IOContent, overwrite: bool = False) -> tuple[int, int]:
        """ TODO """
        self._validate_data(data, self.path)
        added = len(data)
        if overwrite:
            removed = self.__len__()
            self._overwrite(data)
            return added, removed
        else:
            removed = 0
            self._append(data)
            return added, removed

    @abstractmethod
    def _append(self, data: IOContent) -> None:
        """ Append data to the file. """
    
    @abstractmethod
    def _overwrite(self, data: IOContent) -> None:
        """ Overwrite the file with the given data. """

    def save(self) -> None:
        """ Save the current content to the file. """
        self._write_file(self.path, self.content)
        logger.info(f"File {self.path} saved successfully.")

    def delete(self) -> tuple[int, int]:
        """ TODO """
        removed = self.__len__()
        self.content = None
        if self.path.exists():
            if self._confirm_delete():
                self.path.unlink()
                logger.info(f"File {self.path} deleted successfully.")
            else:
                logger.info(f"File deletion for {self.path} was cancelled.")
        else:
            logger.warning(f"File {self.path} does not exist, nothing to delete.")
        return 0, removed

    def _confirm_delete(self) -> bool:
        """ Ask the user to confirm deleting the file. """
        response = input(f"Are you sure you want to delete the file {self.path}? (yes/y to confirm): ").strip().lower()
        return response in ['yes', 'y']

    @staticmethod
    @abstractmethod
    def _read_file(file_path: Path) -> IOContent:
        """ TODO """

    @staticmethod
    @abstractmethod
    def _write_file(file_path: Path, data: IOContent) -> None:
        """ TODO """

    @staticmethod
    @abstractmethod
    def _validate_data(data: IOContent, file_path: Path) -> None:
        """ Validate the data before writing. """
        pass
