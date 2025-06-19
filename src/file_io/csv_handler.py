import pandas as pd

from . import logger
from .base_file_handler import BaseFileHandler


class CSVHandler(BaseFileHandler):
    """ CSV file handler for reading and writing CSV files. """

    def _read_file(self) -> pd.DataFrame:
        """ Read the CSV file and return its content as a DataFrame. """
        if not self.path.exists():
            logger.debug(f"CSV file {self.path} does not exist. Returning empty DataFrame.")
            return pd.DataFrame()
        try:
            return pd.read_csv(self.path, dtype=str)
        except pd.errors.EmptyDataError:
            logger.debug(f"CSV file {self.path} is empty.")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Failed to read CSV file {self.path}: {e}")
            raise e

    def _append(self, data: pd.DataFrame) -> None:
        """ Append data to the CSV file. """
        logger.debug(f"Appending data to CSV file: {self.path}")
        content = self._read_file()
        if not content.empty and set(content.columns) != set(data.columns):
            logger.warning("Data columns do not match existing CSV columns. Appending anyway.")
        content = pd.concat([content, data], ignore_index=True)
        self._write_file(content)

    def _overwrite(self, data: pd.DataFrame) -> None:
        """ Overwrite the CSV file with new data. """
        logger.debug(f"Overwriting CSV file with new data: {self.path}")
        self._write_file(data)

    def _write_file(self, data: pd.DataFrame) -> None:
        """ Write the DataFrame to a CSV file. """
        self._validate_data(data)
        try:
            data.to_csv(self.path, index=False)
        except Exception as e:
            logger.error(f"Failed to write CSV file {self.path}: {e}")
            raise e

    def _get_len(self) -> int:
        """ Return the length of the content. """
        content = self._read_file()
        self._validate_data(content)
        return len(content)

    def _validate_data(self, data: pd.DataFrame) -> None:
        """ TODO """
        if not isinstance(data, pd.DataFrame):
            logger.error(f"Data must be a pandas DataFrame. File handler path: {self.path}")
            raise ValueError(f"Data must be a pandas DataFrame. File handler path: {self.path}")
