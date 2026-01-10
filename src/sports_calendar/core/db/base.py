from __future__ import annotations
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from . import logger
from .filters import Filter, CombinedFilter
from sports_calendar.core.file_io import FileHandlerFactory, CSVHandler


@dataclass(frozen=True)
class Column:
    source: str | None = None
    dtype: type = str
    nullable: bool = False
    format: str | None = None  # only used when dtype is datetime


class TableMeta(type):
    _registry: list[Table] = []  # store all Table subclasses

    def __new__(cls, name, bases, namespace):
        columns = {
            key: value for key, value in namespace.items()
            if isinstance(value, Column)
        }
        namespace["_columns"] = columns
        new_cls = super().__new__(cls, name, bases, namespace)
        if name != "Table":
            cls._registry.append(new_cls)
        return new_cls


class Table(metaclass=TableMeta):
    __file__: str = None
    __sport__: str = None
    _path: Path | None = None # set via set_repo_path
    _columns: dict[str, Column] # populated by TableMeta
    __relationships__: list[Relationship] = []

    @classmethod
    def path(cls) -> Path:
        if cls._path is None:
            logger.error(f"Path for table {cls.__name__} is not set. Please run set_repo_path().")
            raise RuntimeError(f"Path for table {cls.__name__} is not set. Please run set_repo_path().")
        logger.debug(f"Reading path for table {cls.__name__}: {cls._path}")
        return cls._path

    @classmethod
    def file_handler(cls) -> CSVHandler:
        if not hasattr(cls, "_file_handler") or cls._file_handler is None:
            cls._file_handler = FileHandlerFactory.create_file_handler(cls.path())
        return cls._file_handler

    @classmethod
    def get_df(cls) -> pd.DataFrame:
        # TODO - Add caching mechanism later
        df = cls.file_handler().read()

        for col_name, col in cls._columns.items():
            src = col.source or col_name

            if src not in df.columns:
                logger.error(f"Source column {src} not found in table {cls.__name__}.")
                raise KeyError(f"Source column {src} not found in table {cls.__name__}.")

            series = cls._astype(df, col, src)

            df[col_name] = series
            if src != col_name:
                del df[src]

            if not col.nullable and df[col_name].isnull().any():
                logger.error(f"Column {col_name} in table {cls.__name__} contains null values but is marked as non-nullable.")
                raise ValueError(f"Column {col_name} in table {cls.__name__} contains null values but is marked as non-nullable.")
        
        return df

    @classmethod
    def query(cls, *filters: Filter | CombinedFilter) -> pd.DataFrame:
        """
        filters: list of (column, operator, value)
        column can be nested like 'home_team.name'
        operator can be one of ['==', '>=', '<=', 'in', 'contains']
        """
        df = cls.get_df()

        if hasattr(cls, "__relationships__"):
            for rel in cls.__relationships__:
                logger.debug(f"Joining table {cls.__name__} with {rel.table.__name__} on {rel.local_key} = {rel.remote_key}")
                alias = rel.alias
                rel_df = rel.table.get_df()
                # add alias prefix to columns
                rel_df = rel_df.add_prefix(f"{alias}.")
                df = df.merge(
                    rel_df,
                    how="left",
                    left_on=rel.local_key,
                    right_on=f"{alias}.{rel.remote_key}"
                )

        if filters:
            mask = pd.Series([True] * len(df))
            for f in filters:
                if f.col not in df.columns:
                    logger.error(f"Column {f.col} not found in table {cls.__name__}.")
                    raise KeyError(f"Column {f.col} not found in table {cls.__name__}.")
                mask &= f.apply(df)
        return df[mask].copy()

    @staticmethod
    def _astype(df: pd.DataFrame, col: Column, src: str) -> pd.Series:
        if col.dtype is datetime:
            df[src] = pd.to_datetime(
                df[src],
                format=col.format,
                utc=True,
                errors="raise"
            )
        else:
            df[src] = df[src].astype(col.dtype, errors="raise")
        return df[src]


class Relationship:
    def __init__(self, table: type[Table], local_key: str, remote_key: str = "id", alias: str | None = None):
        """
        table: Table class to join with
        local_key: column in THIS table
        remote_key: column in foreign table (default 'id')
        alias: optional prefix for joined table columns
        """
        self.table = table
        self.local_key = local_key
        self.remote_key = remote_key
        self.alias = alias or table.__name__.lower()


def set_repo_path(repo_path: Path, stage: str = "staging"):
    logger.debug(f"Setting repository path to: {repo_path}, stage: {stage}")
    for table_cls in TableMeta._registry:
        if table_cls.__sport__ is None or table_cls.__file__ is None:
            continue
        table_cls._path = repo_path / stage / table_cls.__sport__ / table_cls.__file__
