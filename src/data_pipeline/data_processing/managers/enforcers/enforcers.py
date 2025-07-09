from abc import ABC, abstractmethod
from typing import Generic, TypeVar

import numpy as np
import pandas as pd

from . import logger
from src.specs import ConstraintSpec, UniqueSpec, NonNullableSpec


S = TypeVar("S", bound=ConstraintSpec)

class ConstraintEnforcer(Generic[S], ABC):
    """ Abstract base class for enforcing constraints on DataFrames. """

    def __init__(self, spec: S):
        """ Initialize the enforcer with the specification. """
        self.spec: S = spec

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Apply the constraint enforcement to the given DataFrame. """
        self._check_df(df)
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
