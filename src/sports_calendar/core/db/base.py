from __future__ import annotations
from dataclasses import dataclass
from abc import ABC
from datetime import datetime
from pathlib import Path

import pandas as pd

from . import logger
from .filters import Filter, CombinedFilter
from ..utils import validate
from sports_calendar.core.competition_stages import CompetitionStage
from sports_calendar.core.file_io import FileHandlerFactory, CSVHandler


@dataclass(frozen=True)
class Column:
    source: str
    dtype: type = str
    nullable: bool = False
    format: str | None = None  # only used when dtype is datetime

    def __str__(self) -> str:
        return f"<Column(source={self.source}, dtype={self.dtype.__name__}, nullable={self.nullable}, format={self.format})>"

    def __repr__(self) -> str:
        return self.__str__()


class TableMeta(type):
    _registry: list[type[BaseTable]] = []  # store all Table subclasses

    def __new__(cls, name, bases, namespace):
        columns = {
            key: value for key, value in namespace.items()
            if isinstance(value, Column)
        }
        namespace["_columns"] = columns
        new_cls = super().__new__(cls, name, bases, namespace)
        if name != "Table":
            cls._registry.append(new_cls)
        logger.debug(f"Registered table class: {name} with columns: {list(columns.keys())}")
        logger.debug(f"Current table registry: {[table.__name__ for table in cls._registry]}")
        return new_cls


class BaseTable(ABC, metaclass=TableMeta):
    __table__: str # Name of the table
    __file__: str # Filename where the table data is stored
    __sport__: str # Sport associated with the table
    _columns: dict[str, Column] = {} # Created by TableMeta

    _path: Path | None = None # Created dynamically via set_path()
    _df: pd.DataFrame | None = None # Cached DataFrame

    @classmethod
    def set_path(cls, path: Path):
        cls._path = path

    @classmethod
    def get_path(cls) -> Path:
        validate(bool(cls._path), f"Path for table {cls.__table__} is not set. Please run set_path().", logger, RuntimeError)
        logger.debug(f"Reading path for table {cls.__table__}: {cls._path}")
        return cls._path

    @classmethod
    def file_handler(cls) -> CSVHandler:
        if not hasattr(cls, "_file_handler") or cls._file_handler is None:
            cls._file_handler = FileHandlerFactory.create_file_handler(cls.get_path())
        return cls._file_handler

    @classmethod
    def _view(cls) -> TableView:
        return TableView(cls.get(), cls._columns, cls.__table__)

    @classmethod
    def get(cls) -> pd.DataFrame:
        if cls._df is not None:
            return cls._df.copy()
        df = cls.file_handler().read()

        for col_name, col in cls._columns.items():
            src = col.source
            validate(src in df.columns, f"Source column {src} not found in table {cls.__table__}.", logger, KeyError)
            
            series = cls._astype(df, col, src)
            df[col_name] = series
            if src != col_name:
                del df[src]

            validate(
                col.nullable or not df[col_name].isnull().any(),
                f"Column {col_name} in table {cls.__table__} contains null values but is marked as non-nullable.",
                logger
            )
        
        extra_cols = set(df.columns) - set(cls._columns.keys())
        if extra_cols:
            logger.warning(f"Table {cls.__table__} contains extra columns not defined in schema: {extra_cols}")
            df = df.drop(columns=list(extra_cols))
        
        cls._df = df.copy()
        return df.copy()

    @classmethod
    def select(cls, *columns: str) -> TableView:
        return cls._view().select(*columns)

    @classmethod
    def values(cls, column: str) -> pd.Series:
        return cls._view().values(column)

    @staticmethod
    def _astype(df: pd.DataFrame, col: Column, src: str) -> pd.Series:
        if col.dtype is datetime:
            df[src] = pd.to_datetime(
                df[src],
                format=col.format,
                errors="raise"
            )
        elif col.dtype is CompetitionStage:
            df[src] = df[src].apply(lambda s: CompetitionStage.from_str(s))
        else:
            df[src] = df[src].astype(col.dtype, errors="raise")
        return df[src]

    @classmethod
    def query(cls, *filters: Filter | CombinedFilter) -> TableView:
        return cls._view().query(*filters)

    @classmethod
    def join(
        cls,
        other: type[BaseTable] | TableView,
        left_on: str,
        right_on: str,
        how: str = "left",
        left_alias: str | None = None,
        right_alias: str | None = None
    ) -> TableView:
        return cls._view().join(other, left_on, right_on, how, left_alias, right_alias)

    def __str__(self) -> str:
        return f"<Table({self.__table__}), Columns({list(self._columns.keys())})>"

    def __repr__(self) -> str:
        return f"<Table({self.__table__}), Columns({self._columns}), DataFrame Shape({self._df.shape if self._df is not None else 'Not Loaded'})>"


class TableView:
    def __init__(self, df: pd.DataFrame, columns: dict[str, Column], name: str):
        self._df = df
        self._columns = columns
        self.__table__ = name

    def get(self) -> pd.DataFrame:
        return self._df.copy()

    def select(self, *columns: str) -> TableView:
        validate(all(col in self._columns for col in columns), f"One or more selected columns not found in TableView {self.__table__}.", logger, KeyError)
        selected_df = self._df[list(columns)].copy()
        selected_columns = {col: self._columns[col] for col in columns}
        return TableView(selected_df, selected_columns, f"{self.__table__}_selected")

    def values(self, column: str) -> pd.Series:
        validate(column in self._columns, f"Column {column} not found in TableView {self.__table__}.", logger, KeyError)
        return self._df[column].copy()

    def query(self, *filters: Filter | CombinedFilter) -> TableView:
        df = self.get()

        if filters:
            mask = pd.Series(True, index=df.index)
            for f in filters:
                validate(f.col in df.columns, f"Column {f.col} not found in table {self.__table__}.", logger, KeyError)
                mask &= f.apply(df)
        df = df[mask].copy()

        return TableView(df, self._columns, f"{self.__table__}_filtered")

    def join(
        self,
        other: type[BaseTable] | TableView,
        left_on: str | list[str],
        right_on: str | list[str],
        how: str = "left",
        left_alias: str | None = None,
        right_alias: str | None = None
    ) -> TableView:
        logger.debug(f"Joining TableView {self.__table__} with {other.__str__()} on {left_on} = {right_on} using {how} join.")

        # Prefixes
        left_prefix = f"{left_alias}." if left_alias is not None else f"{self.__table__}."
        right_prefix = f"{right_alias}." if right_alias is not None else f"{other.__table__}."
        validate(left_prefix != right_prefix, "Left and right table aliases must be different to avoid column name collisions.", logger, ValueError)
        left_df = self.get().add_prefix(left_prefix)
        right_df = other.get().add_prefix(right_prefix)

        # Join Keys
        left_keys = [left_on] if isinstance(left_on, str) else list(left_on)
        right_keys = [right_on] if isinstance(right_on, str) else list(right_on)
        validate(len(left_keys) == len(right_keys), "Number of left and right join keys must match.", logger, ValueError)
        left_on = [f"{left_prefix}{key}" for key in left_keys]
        right_on = [f"{right_prefix}{key}" for key in right_keys]

        for k in left_keys:
            validate(
                k in self._columns,
                f"Join key '{k}' not found in left table {self.__table__}.",
                logger,
                KeyError,
            )

        for k in right_keys:
            validate(
                k in other._columns,
                f"Join key '{k}' not found in right table {other.__table__}.",
                logger,
                KeyError,
            )

        # Perform Join
        joined_df = pd.merge(left_df, right_df, left_on=left_on, right_on=right_on, how=how)

        # Get Columns
        columns = {}
        for col_name, col in self._columns.items():
            columns[f"{left_prefix}{col_name}"] = Column(source=f"{left_prefix}{col_name}", dtype=col.dtype, nullable=col.nullable, format=col.format)
        for col_name, col in other._columns.items():
            columns[f"{right_prefix}{col_name}"] = Column(source=f"{right_prefix}{col_name}", dtype=col.dtype, nullable=col.nullable, format=col.format)

        # Create TableView
        return TableView(joined_df, columns, f"{self.__table__}_{other.__table__}_joined")

    def __str__(self) -> str:
        return f"<TableView({self.__table__}), Columns({list(self._columns.keys())})>"

    def __repr__(self) -> str:
        return f"<TableView({self.__table__}), Columns({self._columns}), DataFrame Shape({self._df.shape})>"


def setup_repo_path(repo_path: Path, stage: str = "staging"):
    logger.debug(f"Setting repository path to: {repo_path}, stage: {stage}")
    for table_cls in TableMeta._registry:
        if table_cls.__sport__ is None or table_cls.__file__ is None:
            logger.warning(f"Skipping table {table_cls.__table__} as it lacks __sport__ or __file__ attributes.")
            continue
        table_path = repo_path / stage / table_cls.__sport__ / table_cls.__file__
        table_cls.set_path(table_path)
