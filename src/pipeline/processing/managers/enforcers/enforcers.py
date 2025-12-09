from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Callable

import numpy as np
import pandas as pd

from . import logger
from src.specs import ConstraintSpec, UniqueSpec, NonNullableSpec, CoerceSpec


S = TypeVar("S", bound=ConstraintSpec)

class ConstraintEnforcer(Generic[S], ABC):
    """ Abstract base class for enforcing constraints on DataFrames. """

    def __init__(self, spec: S):
        """ Initialize the enforcer with the specification. """
        self.spec: S = spec

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Apply the constraint enforcement to the given DataFrame. """
        self._check_df(df)
        if df.empty:
            logger.warning("Nothing to enforce: DataFrame is empty.")
            return df
        return self._apply(df)

    @abstractmethod
    def _apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Apply the specific enforcement logic to the DataFrame. """
        raise NotImplementedError("Subclasses must implement this method.")

    @staticmethod
    def _check_df(df: pd.DataFrame) -> None:
        """ Check if df is valid, raise error if not. """
        if not isinstance(df, pd.DataFrame):
            logger.error("Input is not a valid DataFrame.")
            raise ValueError("Input is not a valid DataFrame.")


class UniqueEnforcer(ConstraintEnforcer[UniqueSpec]):
    """ Enforces uniqueness in the DataFrame based on specified fields. """

    def __init__(self, spec: UniqueSpec):
        super().__init__(spec)

    def _apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Apply the specific enforcement logic to the DataFrame. """
        for field_set in self.spec.field_sets:
            logger.debug(f"Enforcing uniqueness for field set: {field_set}")
            df = df.sort_values(by=self.spec.version_col, ascending=True).drop_duplicates(subset=field_set, keep=self.spec.keep)
        return df.reset_index(drop=True)


class NonNullableEnforcer(ConstraintEnforcer[NonNullableSpec]):
    """ Enforces non-nullability in the DataFrame based on specified fields. """

    def __init__(self, spec: NonNullableSpec):
        super().__init__(spec)

    def _apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Apply the specific enforcement logic to the DataFrame. """
        pd.set_option('future.no_silent_downcasting', True)
        df = df.replace({"nan": np.nan, "None": np.nan, "": np.nan})
        return df.dropna(subset=self.spec.fields).reset_index(drop=True)


class CoerceEnforcer(ConstraintEnforcer[CoerceSpec]):
    """ Enforces type constraints on the DataFrame. """

    _CASTERS: dict[str, Callable[[pd.Series], pd.Series]] = {
        "int": lambda s: pd.to_numeric(s, errors="raise").astype("Int64"),
        "float": lambda s: pd.to_numeric(s, errors="raise"),
        "str": lambda s: s.astype(str),
        "bool": lambda s: s.astype(bool),
        "datetime": lambda s: pd.to_datetime(s, errors="raise"),
    }

    def __init__(self, spec: CoerceSpec):
        super().__init__(spec)

    def _apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Apply the specific enforcement logic to the DataFrame. """
        if self.spec.cast_to not in self._CASTERS:
            logger.error(f"Unknown cast type: {self.spec.cast_to}")
            raise ValueError(f"Unknown cast type: {self.spec.cast_to}")

        caster = self._CASTERS[self.spec.cast_to]
        for field in self.spec.fields:
            if field not in df.columns:
                logger.error(f"Field '{field}' not found in DataFrame.")
                raise ValueError(f"Field '{field}' not found in DataFrame.")
            try:
                df[field] = caster(df[field])
            except Exception as e:
                logger.error(f"Failed to coerce field '{field}' to type '{self.spec.cast_to}': {e}")
                raise ValueError(f"Failed to coerce field '{field}' to type '{self.spec.cast_to}': {e}")

        return df.reset_index(drop=True)
