from abc import ABC, abstractmethod

import numpy as np
import pandas as pd

from . import logger
from .specs import UniqueSpec, NonNullableSpec


class OutputEnforcer(ABC):
    """ Abstract base class for enforcing constraints on output DataFrames. """

    @abstractmethod
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Apply the constraint enforcement to the given DataFrame. """

    @staticmethod
    def _check_df(df: pd.DataFrame) -> None:
        """ Check if df is a valid DataFrame, raise error if not. """
        if not isinstance(df, pd.DataFrame):
            logger.error("Input is not a valid DataFrame.")
            raise ValueError("Input is not a valid DataFrame.")


class UniqueEnforcer(OutputEnforcer):
    """ Enforces uniqueness in the DataFrame based on specified fields. """

    def __init__(self, spec: UniqueSpec):
        """ Initialize the enforcer with the specification. """
        self.spec = spec

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Apply uniqueness enforcement to the DataFrame. """
        for field_set in self.spec.field_sets:
            logger.debug(f"Enforcing uniqueness for field set: {field_set}")
            df = df.sort_values(by=self.spec.version_col, ascending=True).drop_duplicates(subset=field_set, keep=self.spec.keep)
        return df.reset_index(drop=True)


class NonNullableEnforcer(OutputEnforcer):
    """ Enforces non-nullability in the DataFrame based on specified fields. """

    def __init__(self, spec: NonNullableSpec):
        """ Initialize the enforcer with the specification. """
        self.spec = spec

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Apply non-nullability enforcement to the DataFrame. """
        pd.set_option('future.no_silent_downcasting', True)
        df = df.replace({"nan": np.nan, "None": np.nan, "": np.nan})
        return df.dropna(subset=self.spec.fields).reset_index(drop=True)
