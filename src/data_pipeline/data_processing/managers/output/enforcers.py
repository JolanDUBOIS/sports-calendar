from abc import ABC, abstractmethod

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
        return (
            df.sort_values(by=self.spec.version_col, ascending=True)
                .drop_duplicates(subset=self.spec.fields, keep=self.spec.keep)
        )


class NonNullableEnforcer(OutputEnforcer):
    """ Enforces non-nullability in the DataFrame based on specified fields. """

    def __init__(self, spec: NonNullableSpec):
        """ Initialize the enforcer with the specification. """
        self.spec = spec

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Apply non-nullability enforcement to the DataFrame. """
        return df.dropna(subset=self.spec.fields)
