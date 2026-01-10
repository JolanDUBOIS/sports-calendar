from __future__ import annotations
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from abc import ABC, abstractmethod

import pandas as pd

from . import logger
from .filters import Filter, CombinedFilter
from ..utils import validate
from sports_calendar.core.file_io import FileHandlerFactory, CSVHandler


@dataclass(frozen=True)
class Column:
    source: str
    dtype: type = str
    nullable: bool = False
    format: str | None = None  # only used when dtype is datetime


class TableMeta(type):
    _registry: list[type[Table]] = []  # store all Table subclasses

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


class AbstractTable(ABC):
    __table__: str
    _columns: dict[str, Column]

    @abstractmethod
    def get_df(cls) -> pd.DataFrame: ...

    @abstractmethod
    def join(
        cls,
        other: AbstractTable | type[AbstractTable],
        left_on: str,
        right_on: str,
        how: str = "left",
        prefix: str | None = None
    ) -> JoinedTable: ...

    @abstractmethod
    def query(cls, *filters: Filter | CombinedFilter) -> pd.DataFrame: ...


class Table(AbstractTable, metaclass=TableMeta):
    __table__: str # name of the table
    __file__: str | None = None
    __sport__: str | None = None
    _path: Path | None = None # set via set_repo_path
    _columns: dict[str, Column] # populated by TableMeta

    @classmethod
    def path(cls) -> Path:
        validate(bool(cls._path), f"Path for table {cls.__table__} is not set. Please run set_repo_path().", logger, RuntimeError)
        logger.debug(f"Reading path for table {cls.__table__}: {cls._path}")
        return cls._path

    @classmethod
    def file_handler(cls) -> CSVHandler:
        if not hasattr(cls, "_file_handler") or cls._file_handler is None:
            cls._file_handler = FileHandlerFactory.create_file_handler(cls.path())
        return cls._file_handler

    @classmethod
    def get_df(cls) -> pd.DataFrame:
        if hasattr(cls, "_cached_df") and cls._cached_df is not None:
            return cls._cached_df.copy()

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

        cls._cached_df = df
        return df.copy()

    @classmethod
    def join(cls, other: AbstractTable, left_on: str, right_on: str, how: str = "left") -> JoinedTable:
        logger.debug(f"Joining table {cls.__str__()} with {other.__str__()} on {left_on} = {right_on} using {how} join.")
        left_df = cls.get_df().add_prefix(f"{cls.__table__}.")
        if isinstance(other, type) and issubclass(other, Table):
            right_df = other.get_df().add_prefix(f"{other.__table__}.")
        elif isinstance(other, JoinedTable):
            right_df = other.get_df().add_prefix(f"{other.__table__}.")
        else:
            logger.error(f"Invalid type for 'other' in join: {type(other)}")
            raise TypeError(f"Invalid type for 'other' in join: {type(other)}")
        
        joined_df = pd.merge(left_df, right_df, left_on=left_on, right_on=right_on, how=how)
        tables = (cls, other)
        return JoinedTable(joined_df, tables=tables)

    @classmethod
    def query(cls, *filters: Filter | CombinedFilter) -> pd.DataFrame:
        """
        filters: list of (column, operator, value)
        column can be nested like 'home_team.name'
        operator can be one of ['==', '>=', '<=', 'in', 'contains']
        """
        df = cls.get_df()

        if filters:
            mask = pd.Series([True] * len(df))
            for f in filters:
                validate(f.col in df.columns, f"Column {f.col} not found in table {cls.__table__}.", logger, KeyError)
                mask &= f.apply(df)
        return df[mask].copy()

    @staticmethod
    def _astype(df: pd.DataFrame, col: Column, src: str) -> pd.Series:
        if col.dtype is datetime:
            df[src] = pd.to_datetime(
                df[src],
                format=col.format,
                errors="raise"
            )
        else:
            df[src] = df[src].astype(col.dtype, errors="raise")
        return df[src]

    def __str__(cls):
        return f"<Table({cls.__table__})>"

    def __repr__(cls):
        return f"<Table({cls.__table__}), Columns({list(cls._columns.keys())})>"


class JoinedTable(AbstractTable):
    def __init__(self, df: pd.DataFrame, name: str, tables: tuple[AbstractTable, AbstractTable]):
        self._df = df
        self.__table__ = name

        validate(
            all(isinstance(t, (type, JoinedTable)) and issubclass(t, Table) if isinstance(t, type) else isinstance(t, JoinedTable) for t in tables),
            "All elements in tables must be Table subclasses or JoinedTable instances.",
            logger,
            TypeError
        )
        self._tables = tables
        self.init_cols()

    def init_cols(self):
        self._columns = {}
        for table in self._tables:
            for col, column in table._columns.items():
                self._columns[f"{table.__table__}.{col}"] = Column(
                    source=f"{table.__table__}.{col}",
                    dtype=column.dtype,
                    nullable=column.nullable,
                    format=column.format
                )

    def get_df(self) -> pd.DataFrame:
        for _, col in self._columns.items():
            src = col.source
            validate(src in self._df.columns, f"Source column {src} not found in table {self.__table__}.", logger, KeyError)
        return self._df.copy()

    def join(self, other: AbstractTable, left_on: str, right_on: str, how: str = "left") -> JoinedTable:
        logger.debug(f"Joining JoinedTable with {other.__str__()} on {left_on} = {right_on} using {how} join.")
        left_df = self.get_df().add_prefix(f"{self.__table__}.")
        if isinstance(other, type) and issubclass(other, Table):
            right_df = other.get_df().add_prefix(f"{other.__table__}.")
        elif isinstance(other, JoinedTable):
            right_df = other.get_df().add_prefix(f"{other.__table__}.")
        else:
            logger.error(f"Invalid type for 'other' in join: {type(other)}")
            raise TypeError(f"Invalid type for 'other' in join: {type(other)}")
        
        joined_df = pd.merge(left_df, right_df, left_on=left_on, right_on=right_on, how=how)
        tables = (self, other)
        return JoinedTable(joined_df, tables=tables)

    def query(self, *filters: Filter | CombinedFilter) -> pd.DataFrame:
        if not filters:
            return self._df.copy()
        mask = pd.Series([True] * len(self._df))
        for f in filters:
            validate(f.col in self._df.columns, f"Column {f.col} not found in joined table.", logger, KeyError)
            mask &= f.apply(self._df)
        return self._df[mask].copy()

    def __str__(self):
        tables_str = " | ".join(table.__table__ for table in self._tables)
        return f"<JoinedTable({tables_str})>"

    def __repr__(self):
        tables_str = " | ".join(table.__table__ for table in self._tables)
        return f"<JoinedTable({tables_str}), Columns({list(self._df.columns)})>"
