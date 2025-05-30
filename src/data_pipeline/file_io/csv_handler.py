from pathlib import Path

import pandas as pd

from . import logger, FileHandler


class CSVHandler(FileHandler):
    """ CSV file handler for reading and writing CSV files. """

    def _append(self, data: pd.DataFrame) -> None:
        """ Append data to the CSV file. """
        if not self.content.empty and set(self.content.columns) != set(data.columns):
            logger.error("Data columns do not match existing CSV columns.")
            raise ValueError("Data columns do not match existing CSV columns.")
        self.content = pd.concat([self.content, data], ignore_index=True)

    def _overwrite(self, data: pd.DataFrame) -> None:
        """ Overwrite the CSV file with new data. """
        if not self.content.empty and set(self.content.columns) != set(data.columns):
            logger.warning("Data columns do not match existing CSV columns. Overwriting with new data.")
        self.content = data

    @staticmethod
    def _read_file(file_path: Path) -> pd.DataFrame:
        """ Read the CSV file and return its content as a DataFrame. """
        try:
            return pd.read_csv(file_path)
        except pd.errors.EmptyDataError:
            logger.debug(f"CSV file {file_path} is empty.")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Failed to read CSV file {file_path}: {e}")
            raise e

    @staticmethod
    def _write_file(file_path: Path, data: pd.DataFrame) -> None:
        """ Write the DataFrame to a CSV file. """
        if data is None or data.empty:
            logger.debug(f"Data is None or empty. File handler path: {file_path}")
            return
        try:
            data.to_csv(file_path, index=False)
        except Exception as e:
            logger.error(f"Failed to write CSV file {file_path}: {e}")
            raise e

    @staticmethod
    def _validate_data(data: pd.DataFrame, file_path: Path) -> None:
        """ TODO """
        if not isinstance(data, pd.DataFrame):
            logger.error(f"Data must be a pandas DataFrame. File handler path: {file_path}")
            raise ValueError(f"Data must be a pandas DataFrame. File handler path: {file_path}")
