from pathlib import Path
from abc import ABC, abstractmethod

import pandas as pd

from . import logger
from src.file_io import FileHandlerFactory


class BaseTable(ABC):
    """ Base class for all tables. """
    __file_name__ = None
    __columns__ = None
    __sport__ = None
    _file_handler = None
    _repo_path = None

    @classmethod
    def configure(cls, repo_path: Path):
        cls._repo_path = repo_path

    @classmethod
    @abstractmethod
    def query(cls, **kwargs) -> pd.DataFrame:
        """ Abstract method to query data from the table. """
        raise NotImplementedError(f"{cls.__name__}.query() is not implemented.")

    @classmethod
    def get_table(cls) -> pd.DataFrame:
        """ Returns the table as a DataFrame. """
        if cls.__file_name__ is None:
            logger.error(f"File name for {cls.__name__} is not defined.")
            raise ValueError(f"File name for {cls.__name__} is not defined.")
        if cls.__columns__ is None:
            logger.error(f"Columns for {cls.__name__} are not defined.")
            raise ValueError(f"Columns for {cls.__name__} are not defined.")

        if cls._file_handler is None:
            path = cls._repo_path / "staging" / cls.__sport__ / cls.__file_name__
            cls._file_handler = FileHandlerFactory.create_file_handler(path)

        df = cls._file_handler.read()
        if df.empty:
            logger.warning(f"The table {cls.__name__} is empty.")
            return df
        df = cls._as_types(df, cls.__columns__)
        return df

    @staticmethod
    def _as_types(df: pd.DataFrame, columns: dict) -> pd.DataFrame:
        """ Convert DataFrame columns to specified types. """
        rename_map = {v["source"]: k for k, v in columns.items() if v["source"] is not None}
        df = df[list(rename_map.keys())]
        df = df.rename(columns=rename_map)

        for col, props in columns.items():
            src = props.get("source")
            if col not in df.columns:
                if src is None:
                    df[col] = None
                    continue
                else:
                    logger.error(f"Column '{col}' not found in DataFrame.")
                    raise ValueError(f"Column '{col}' not found in DataFrame.")

            col_type = props.get("type")
            if not col_type:
                logger.warning(f"Column '{col}' has no type specified, skipping conversion.")
                continue

            try:
                if col_type == "int":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif col_type == "float":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
                elif col_type == "bool":
                    df[col] = df[col].map({'True': True, 'False': False})
                    df[col] = df[col].astype("boolean")  # Pandas nullable bool
                else:
                    df[col] = df[col].astype(col_type)
            except Exception as e:
                logger.error(f"Error converting column '{col}' to {col_type}: {e}")
                logger.debug(f"DataFrame columns: {df.columns.tolist()}")
                logger.debug(f"DataFrame head:\n{df.head()}")
                raise
        
        return df[[col for col in columns if col in df]]
